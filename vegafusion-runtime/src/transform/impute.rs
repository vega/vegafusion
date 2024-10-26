use crate::expression::compiler::config::CompilationConfig;

use crate::data::util::DataFrameUtils;
use crate::expression::compiler::utils::ExprHelpers;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_common::{JoinType, ScalarValue};
use datafusion_expr::{expr, lit, Expr, SortExpr, WindowFrame, WindowFunctionDefinition};
use datafusion_functions::expr_fn::coalesce;
use datafusion_functions_aggregate::expr_fn::min;
use datafusion_functions_window::row_number::RowNumber;
use itertools::Itertools;
use sqlparser::ast::NullTreatment;
use std::sync::Arc;
use vegafusion_common::column::{flat_col, relation_col};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::{Result, ResultWithContext};
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::Impute;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Impute {
    async fn eval(
        &self,
        dataframe: DataFrame,
        _config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
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

        // Take unique groupby fields (in case there are duplicates)
        let groupby = self
            .groupby
            .clone()
            .into_iter()
            .unique()
            .collect::<Vec<_>>();

        // Unescape field, key, and groupby fields
        let field = unescape_field(&self.field);
        let key = unescape_field(&self.key);
        let groupby: Vec<_> = groupby.iter().map(|f| unescape_field(f)).collect();

        let schema = dataframe.schema();
        let (_, field_field) = schema
            .inner()
            .column_with_name(&field)
            .with_context(|| format!("No field named {}", field))?;
        let field_type = field_field.data_type();

        if groupby.is_empty() {
            // Value replacement for field with no group_by fields specified is equivalent to replacing
            // null values of that column with the fill value
            let select_columns = schema
                .fields()
                .iter()
                .map(|f| {
                    let col_name = f.name();
                    Ok(if col_name == &field {
                        coalesce(vec![
                            flat_col(&field),
                            lit(value.clone()).try_cast_to(field_type, schema)?,
                        ])
                        .alias(col_name)
                    } else {
                        flat_col(col_name)
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok((dataframe.select(select_columns)?, Vec::new()))
        } else {
            // First step is to build up a new DataFrame that contains the all possible combinations

            // Build some internal columns for intermediate ordering
            let order_col = flat_col(ORDER_COL);
            let order_key = format!("{ORDER_COL}_key");
            let order_key_col = flat_col(&order_key);
            let order_group = format!("{ORDER_COL}_groups");
            let order_group_col = flat_col(&order_group);

            // Create DataFrame with unique key values, and an internal ordering column
            let key_col = flat_col(&key);
            let key_df = dataframe
                .clone()
                .filter(key_col.clone().is_not_null())?
                .aggregate_mixed(
                    vec![key_col.clone()],
                    vec![min(order_col.clone()).alias(&order_key)],
                )?;

            // Create DataFrame with unique combinations of group_by values, with an
            // internal ordering col
            let group_cols = groupby.iter().map(|c| flat_col(c)).collect::<Vec<_>>();

            let groups_df = dataframe
                .clone()
                .aggregate_mixed(group_cols, vec![min(order_col.clone()).alias(&order_group)])?;

            // Build join conditions
            let mut on_exprs = groupby
                .iter()
                .map(|c| relation_col(c, "lhs").eq(relation_col(c, "rhs")))
                .collect::<Vec<_>>();
            on_exprs.push(relation_col(&key, "lhs").eq(relation_col(&key, "rhs")));

            let pre_ordered_df = key_df
                .join_on(groups_df, JoinType::Inner, vec![])?
                .alias("lhs")?
                .join_on(dataframe.clone().alias("rhs")?, JoinType::Left, on_exprs)?;

            // Build final selection that fills in missing values and adds ordering column
            let mut final_selections = Vec::new();
            for field_index in 0..schema.fields().len() {
                let (_, f) = schema.qualified_field(field_index);

                if f.name().starts_with(ORDER_COL) {
                    // Skip all order cols
                    continue;
                } else if f.name() == &field {
                    // Coalesce to fill in null values in field
                    final_selections.push(
                        coalesce(vec![
                            flat_col(&field),
                            lit(value.clone()).try_cast_to(field_type, schema)?,
                        ])
                        .alias(f.name()),
                    );
                } else {
                    // Keep other columns
                    if f.name() == &key || groupby.contains(f.name()) {
                        // Pull key and groupby columns from the "lhs" table (which won't have nulls
                        // introduced by the left join)
                        final_selections.push(relation_col(f.name(), "lhs"));
                    } else {
                        // Pull all other columns from the rhs table
                        final_selections.push(relation_col(f.name(), "rhs"));
                    }
                }
            }

            let final_order_expr = Expr::WindowFunction(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(Arc::new(RowNumber::new().into())),
                args: vec![],
                partition_by: vec![],
                order_by: vec![
                    // Sort first by the original row order, pushing imputed rows to the end
                    SortExpr::new(order_col.clone(), true, false),
                    // Sort imputed rows by first row that resides group
                    // then by first row that matches a key
                    SortExpr::new(order_group_col, true, true),
                    SortExpr::new(order_key_col, true, true),
                ],
                window_frame: WindowFrame::new(Some(true)),
                null_treatment: Some(NullTreatment::RespectNulls),
            })
            .alias(ORDER_COL);
            final_selections.push(final_order_expr);

            Ok((pre_ordered_df.select(final_selections)?, Default::default()))
        }
    }
}
