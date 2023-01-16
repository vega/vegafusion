use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Fold;

use crate::expression::escape::flat_col;
use crate::sql::compile::select::ToSqlSelectItem;
use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use datafusion_expr::{
    expr, lit, window_function, BuiltInWindowFunction, Expr, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use sqlgen::dialect::DialectDisplay;
use vegafusion_core::data::ORDER_COL;
use vegafusion_core::expression::escape::unescape_field;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Fold {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Extract key and value columns
        let dialect = dataframe.dialect();
        let key_col = unescape_field(
            &self
                .r#as
                .get(0)
                .cloned()
                .unwrap_or_else(|| "key".to_string()),
        );
        let value_col = unescape_field(
            &self
                .r#as
                .get(1)
                .cloned()
                .unwrap_or_else(|| "value".to_string()),
        );
        let field_order_col = format!("{}_field", ORDER_COL);

        // Build selection that includes all input fields that aren't shadowed by key/value cols
        let input_selection = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|field| {
                if field.name() == &key_col || field.name() == &value_col {
                    None
                } else {
                    Some(flat_col(field.name()))
                }
            })
            .collect::<Vec<_>>();

        // Build query per field
        let subqueries = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                // Clone input selection and add key/val cols to it
                let mut subquery_selection = input_selection.clone();
                subquery_selection.push(lit(field).alias(key_col.clone()));
                if dataframe.schema().column_with_name(field).is_some() {
                    // Field exists as a column in the parent table
                    subquery_selection.push(flat_col(field).alias(value_col.clone()));
                } else {
                    // Field does not exist in parent table, fill in NULL instead
                    subquery_selection.push(lit(ScalarValue::Null).alias(value_col.clone()));
                }
                subquery_selection.push(lit(i as u32).alias(field_order_col.clone()));

                // Create selection CSV for subquery
                let selection_strs = subquery_selection
                    .iter()
                    .map(|sel| Ok(sel.to_sql_select()?.sql(dialect)?))
                    .collect::<Result<Vec<_>>>()?;
                let selection_csv = selection_strs.join(", ");

                Ok(format!(
                    "(SELECT {selection_csv} from {parent})",
                    selection_csv = selection_csv,
                    parent = dataframe.parent_name()
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let union_subquery = subqueries.join(" UNION ALL ");
        let union_subquery_name = "_union";

        let mut selections = input_selection.clone();
        selections.push(flat_col(&key_col));
        selections.push(flat_col(&value_col));
        selections.push(flat_col(&field_order_col));

        let selection_strs = selections
            .iter()
            .map(|sel| Ok(sel.to_sql_select()?.sql(dialect)?))
            .collect::<Result<Vec<_>>>()?;
        let selection_csv = selection_strs.join(", ");

        let sql = format!(
            "SELECT {selection_csv} FROM ({union_subquery}) as {union_subquery_name}",
            selection_csv = selection_csv,
            union_subquery = union_subquery,
            union_subquery_name = union_subquery_name
        );
        let dataframe = dataframe.chain_query_str(&sql).await?;

        // Add new ordering column, ordering by:
        // 1. input row ordering
        // 2. field index
        let order_col = Expr::WindowFunction(expr::WindowFunction {
            fun: window_function::WindowFunction::BuiltInWindowFunction(
                BuiltInWindowFunction::RowNumber,
            ),
            args: vec![],
            partition_by: vec![],
            order_by: vec![
                Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col(ORDER_COL)),
                    asc: true,
                    nulls_first: false,
                }),
                Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col(&field_order_col)),
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

        // Build output selections
        let mut selections = input_selection.clone();
        selections.push(flat_col(&key_col));
        selections.push(flat_col(&value_col));
        selections[0] = order_col;
        let result = dataframe.select(selections).await?;

        Ok((result, Default::default()))
    }
}
