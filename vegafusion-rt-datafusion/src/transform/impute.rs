use crate::expression::compiler::config::CompilationConfig;
use crate::expression::escape::{flat_col, unescaped_col};
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use datafusion_expr::{
    expr, lit, when, window_function, BuiltInWindowFunction, Expr, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use itertools::Itertools;
use sqlgen::dialect::DialectDisplay;
use std::sync::Arc;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::data::ORDER_COL;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Impute;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Impute {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Create ScalarValue used to fill in null values
        let json_value: serde_json::Value = serde_json::from_str(
            &self
                .value_json
                .clone()
                .unwrap_or_else(|| "null".to_string()),
        )?;

        // JSON numbers are always interpreted as floats, but if the value is an integer we'd
        // like the fill value to be an integer as well to avoid converting an integer input
        // column to floats
        let value = if json_value.is_null() {
            ScalarValue::Float64(None)
        } else if json_value.is_i64() {
            ScalarValue::from(json_value.as_i64().unwrap())
        } else if json_value.is_f64() && json_value.as_f64().unwrap().fract() == 0.0 {
            ScalarValue::from(json_value.as_f64().unwrap() as i64)
        } else {
            ScalarValue::from_json(&json_value)?
        };

        let unique_groupby = self
            .groupby
            .clone()
            .into_iter()
            .unique()
            .collect::<Vec<_>>();
        let dataframe = match unique_groupby.len() {
            0 => impute_without_groupby(self, dataframe, value).await?,
            _ => impute_with_groupby(self, dataframe, value, unique_groupby.as_slice()).await?,
        };

        Ok((dataframe, Vec::new()))
    }
}

async fn impute_without_groupby(
    tx: &Impute,
    dataframe: Arc<SqlDataFrame>,
    value: ScalarValue,
) -> Result<Arc<SqlDataFrame>> {
    // Value replacement for field with no groupby fields specified is equivalent to replacing
    // null values of that column with the fill value
    let select_columns: Vec<_> = dataframe
        .schema_df()
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name();
            if col_name == &tx.field {
                when(
                    unescaped_col(&tx.field).is_not_null(),
                    unescaped_col(&tx.field),
                )
                .otherwise(lit(value.clone()))
                .unwrap()
                .alias(&tx.field)
            } else {
                unescaped_col(col_name)
            }
        })
        .collect();

    dataframe.select(select_columns).await
}

async fn impute_with_groupby(
    tx: &Impute,
    dataframe: Arc<SqlDataFrame>,
    value: ScalarValue,
    groupby: &[String],
) -> Result<Arc<SqlDataFrame>> {
    // Save off names of columns in the original input DataFrame
    let original_columns: Vec<_> = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();

    // First step is to build up a new DataFrame that contains the all possible combinations
    // of the `key` and `groupby` columns
    let key_col = unescaped_col(&tx.key);
    let key_col_str = key_col.to_sql_select()?.sql(dataframe.dialect())?;

    let group_cols = groupby.iter().map(|c| unescaped_col(c)).collect::<Vec<_>>();
    let group_col_strs = group_cols
        .iter()
        .map(|c| Ok(c.to_sql_select()?.sql(dataframe.dialect())?))
        .collect::<Result<Vec<_>>>()?;
    let group_cols_csv = group_col_strs.join(", ");

    // Build final selection
    // Finally, select all of the original DataFrame columns, filling in missing values
    // of the `field` columns
    let select_columns: Vec<_> = original_columns
        .iter()
        .map(|col_name| {
            if col_name == &tx.field {
                when(
                    unescaped_col(&tx.field).is_not_null(),
                    unescaped_col(&tx.field),
                )
                .otherwise(lit(value.clone()))
                .unwrap()
                .alias(&tx.field)
            } else {
                unescaped_col(col_name)
            }
        })
        .collect();

    let select_column_strs = select_columns
        .iter()
        .map(|c| Ok(c.to_sql_select()?.sql(dataframe.dialect())?))
        .collect::<Result<Vec<_>>>()?;

    let select_column_csv = select_column_strs.join(", ");

    let mut using_strs = vec![key_col_str.clone()];
    using_strs.extend(group_col_strs.clone());
    let using_csv = using_strs.join(", ");

    let sql = format!(
        "SELECT {select_column_csv}, {ORDER_COL}_key, {ORDER_COL}_groups \
         FROM (SELECT {key}, min({ORDER_COL}) as {ORDER_COL}_key from {parent} WHERE {key} IS NOT NULL GROUP BY {key}) AS _key \
         CROSS JOIN (SELECT {group_cols_csv}, min({ORDER_COL}) as {ORDER_COL}_groups from {parent} GROUP BY {group_cols_csv}) as _groups \
         LEFT OUTER JOIN {parent} \
         USING ({using_csv})",
        select_column_csv = select_column_csv,
        group_cols_csv = group_cols_csv,
        key = key_col_str,
        using_csv = using_csv,
        ORDER_COL = ORDER_COL,
        parent = dataframe.parent_name(),
    );
    let dataframe = dataframe.chain_query_str(&sql).await?;

    // Override ordering column since null values may have been introduced in the query above.
    // Match input ordering with imputed rows (those will null ordering column) pushed
    // to the end.
    let order_col = Expr::WindowFunction(expr::WindowFunction {
        fun: window_function::WindowFunction::BuiltInWindowFunction(
            BuiltInWindowFunction::RowNumber,
        ),
        args: vec![],
        partition_by: vec![],
        order_by: vec![
            // Sort first by the original row order, pushing imputed rows to the end
            Expr::Sort(expr::Sort {
                expr: Box::new(flat_col(ORDER_COL)),
                asc: true,
                nulls_first: false,
            }),
            // Sort imputed rows by first row that resides group
            // then by first row that matches a key
            Expr::Sort(expr::Sort {
                expr: Box::new(flat_col(&format!("{}_groups", ORDER_COL))),
                asc: true,
                nulls_first: false,
            }),
            Expr::Sort(expr::Sort {
                expr: Box::new(flat_col(&format!("{}_key", ORDER_COL))),
                asc: true,
                nulls_first: false,
            }),
        ],
        window_frame: WindowFrame {
            units: WindowFrameUnits::Rows,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::CurrentRow,
        },
    })
    .alias(ORDER_COL);

    // Build vector of selections
    let mut selections = dataframe
        .schema()
        .fields
        .iter()
        .filter_map(|field| {
            if field.name().starts_with(ORDER_COL) {
                None
            } else {
                Some(flat_col(field.name()))
            }
        })
        .collect::<Vec<_>>();
    selections.insert(0, order_col);

    dataframe
        .select(selections)
        .await
        .with_context(|| "Impute transform failed".to_string())
}
