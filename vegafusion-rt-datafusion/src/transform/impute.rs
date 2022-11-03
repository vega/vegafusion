use crate::expression::compiler::config::CompilationConfig;
use crate::expression::escape::{flat_col, unescaped_col};
use crate::sql::compile::order::ToSqlOrderByExpr;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use datafusion_expr::{lit, when, BuiltInWindowFunction, Expr, WindowFunction};
use sqlgen::dialect::DialectDisplay;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::data::scalar::ScalarValueHelpers;
use vegafusion_core::error::{Result, VegaFusionError};
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
        let json_value: serde_json::Value =
            serde_json::from_str(self.value_json.as_ref().unwrap())?;

        // JSON numbers are always interpreted as floats, but if the value is an integer we'd
        // like the fill value to be an integer as well to avoid converting an integer input
        // column to floats
        let value = if json_value.is_i64() {
            ScalarValue::from(json_value.as_i64().unwrap())
        } else if json_value.is_f64() && json_value.as_f64().unwrap().fract() == 0.0 {
            ScalarValue::from(json_value.as_f64().unwrap() as i64)
        } else {
            ScalarValue::from_json(&json_value)?
        };

        let dataframe = match self.groupby.len() {
            0 => zero_groupby_sql(self, dataframe, value)?,
            1 => single_groupby_sql(self, dataframe, value)?,
            _ => {
                return Err(VegaFusionError::internal(
                    "Expected zero or one groupby columns to impute",
                ))
            }
        };

        Ok((dataframe, Vec::new()))
    }
}

fn zero_groupby_sql(
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

    dataframe.select(select_columns)
}

fn single_groupby_sql(
    tx: &Impute,
    dataframe: Arc<SqlDataFrame>,
    value: ScalarValue,
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

    // We're only supporting a single groupby column for now
    let groupby = tx.groupby.get(0).unwrap().clone();

    let key_col = unescaped_col(&tx.key);
    let key_col_str = key_col.to_sql_select()?.sql(dataframe.dialect())?;

    let group_col = unescaped_col(&groupby);
    let group_col_str = group_col.to_sql_select()?.sql(dataframe.dialect())?;

    // Build row number expr to apply to input table
    let row_number_expr = Expr::WindowFunction {
        fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
        args: Vec::new(),
        partition_by: Vec::new(),
        order_by: Vec::new(),
        window_frame: None,
    }
    .alias("__row_number");
    let row_number_expr_str = row_number_expr.to_sql_select()?.sql(dataframe.dialect())?;

    // Build order by
    let order_by_expr = Expr::Sort {
        expr: Box::new(flat_col("__row_number")),
        asc: true,
        nulls_first: false,
    };
    let order_by_expr_str = order_by_expr.to_sql_order()?.sql(dataframe.dialect())?;

    // Build final selection
    // Finally, select all of the original DataFrame columns, filling in missing values
    // of the `field` columns
    let mut select_columns: Vec<_> = original_columns
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

    // Add undocumented "_impute" column that Vega adds
    select_columns.push(
        when(
            unescaped_col(&tx.field).is_not_null(),
            Expr::Cast {
                expr: Box::new(Expr::Literal(ScalarValue::Boolean(None))),
                data_type: DataType::Boolean,
            },
        )
        .otherwise(lit(true))
        .unwrap()
        .alias("_impute"),
    );
    let select_column_strs = select_columns
        .iter()
        .map(|c| Ok(c.to_sql_select()?.sql(dataframe.dialect())?))
        .collect::<Result<Vec<_>>>()?;

    let select_column_csv = select_column_strs.join(", ");

    let dataframe = dataframe.chain_query_str(&format!(
        "SELECT {select_column_csv} from (SELECT DISTINCT {key} from {parent} WHERE {key} IS NOT NULL) AS _key \
         CROSS JOIN (SELECT DISTINCT {group} from {parent} WHERE {group} IS NOT NULL) AS _group  \
         LEFT OUTER JOIN (SELECT *, {row_number_expr_str} from {parent}) AS _inner USING ({key}, {group}) \
         ORDER BY {order_by_expr_str}",
        select_column_csv = select_column_csv,
        key = key_col_str,
        group = group_col_str,
        row_number_expr_str = row_number_expr_str,
        order_by_expr_str = order_by_expr_str,
        parent = dataframe.parent_name(),
    ))?;

    Ok(dataframe)
}
