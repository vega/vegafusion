use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::ScalarValue;
use datafusion_expr::{expr, lit, Expr, WindowFrame, WindowFunctionDefinition};
use datafusion_functions_window::row_number::RowNumber;
use sqlparser::ast::NullTreatment;
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::Result;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::Fold;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Fold {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let field_cols: Vec<_> = self.fields.iter().map(|f| unescape_field(f)).collect();
        let key_col = unescape_field(
            &self
                .r#as
                .first()
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

        // Build selection that includes all input fields that
        // aren't shadowed by key/value cols
        let input_selection = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == &key_col || f.name() == &value_col {
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            })
            .collect::<Vec<_>>();

        // Build union of subqueries that select and rename each field
        let mut subquery_union: Option<DataFrame> = None;

        let field_order_col = format!("{ORDER_COL}_field");
        for (i, field) in field_cols.iter().enumerate() {
            // Clone input selection and add key/val cols to it
            let mut subquery_selection = input_selection.clone();
            subquery_selection.push(lit(field).alias(key_col.clone()));
            if dataframe.schema().inner().column_with_name(field).is_some() {
                // Field exists as a column in the parent table
                subquery_selection.push(flat_col(field).alias(value_col.clone()));
            } else {
                // Field does not exist in parent table, fill in NULL instead
                subquery_selection.push(lit(ScalarValue::Null).alias(value_col.clone()));
            }

            // Add order column
            subquery_selection.push(lit(i as u32).alias(&field_order_col));

            let subquery_df = dataframe.clone().select(subquery_selection)?;
            if let Some(union) = subquery_union {
                subquery_union = Some(union.union(subquery_df)?);
            } else {
                subquery_union = Some(subquery_df);
            }
        }

        // Unwrap
        let Some(subquery_union) = subquery_union else {
            // Return input dataframe as-is
            return Ok((dataframe, Default::default()));
        };

        // Compute final selection, start with all the non-order input columns
        let mut final_selections = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == ORDER_COL {
                    None
                } else {
                    Some(flat_col(f.name()))
                }
            })
            .collect::<Vec<_>>();

        // Add key and value columns
        final_selections.push(flat_col(&key_col));
        final_selections.push(flat_col(&value_col));

        // Add new order column
        let final_order_expr = Expr::WindowFunction(expr::WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
            args: vec![],
            partition_by: vec![],
            order_by: vec![
                expr::Sort {
                    expr: flat_col(ORDER_COL),
                    asc: true,
                    nulls_first: true,
                },
                expr::Sort {
                    expr: flat_col(&field_order_col),
                    asc: true,
                    nulls_first: true,
                },
            ],
            window_frame: WindowFrame::new(Some(true)),
            null_treatment: Some(NullTreatment::IgnoreNulls),
        })
        .alias(ORDER_COL);
        final_selections.push(final_order_expr);

        Ok((subquery_union.select(final_selections)?, Default::default()))
    }
}
