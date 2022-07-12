/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::to_numeric;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::physical_plan::aggregates;
use datafusion_expr::{case, col, lit, when, BuiltInWindowFunction, Expr, WindowFunction};
use std::ops::{Add, Sub};
use std::sync::Arc;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::{SortOrder, Stack};
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Stack {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        // Assume offset is Zero until others are implemented

        let alias0 = self.alias_0.clone().expect("alias0 expected");
        let alias1 = self.alias_1.clone().expect("alias1 expected");

        let mut order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| Expr::Sort {
                expr: Box::new(col(field)),
                asc: *order == SortOrder::Ascending as i32,
                nulls_first: *order == SortOrder::Ascending as i32,
            })
            .collect();

        // Save off input columns
        let input_fields: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|f| f.field().name().clone())
            .collect();

        // Build first selection
        let mut selection_0: Vec<_> = input_fields
            .iter()
            .filter_map(|field| {
                if field == &alias0 || field == &alias1 {
                    None
                } else {
                    Some(col(field))
                }
            })
            .collect();

        //  If no order by fields provided, use the row number
        let row_number_expr = Expr::WindowFunction {
            fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
            args: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: None,
        }
        .alias("__row_number");
        selection_0.push(col("__row_number"));
        let dataframe = dataframe.select(vec![Expr::Wildcard, row_number_expr])?;

        // Order by row number last (and only if no explicit ordering provided)
        order_by.push(Expr::Sort {
            expr: Box::new(col("__row_number")),
            asc: true,
            nulls_first: true,
        });

        // Build groupby columns
        let partition_by: Vec<_> = self.groupby.iter().map(|group| col(group)).collect();

        // Build window expression
        let fun = WindowFunction::AggregateFunction(aggregates::AggregateFunction::Sum);

        // Case field to number and replace with 0 when null
        let numeric_field = to_numeric(col(&self.field), dataframe.schema())?;
        let numeric_field =
            when(col(&self.field).is_not_null(), numeric_field).otherwise(lit(0.0))?;

        let window_expr = Expr::WindowFunction {
            fun,
            args: vec![numeric_field.clone()],
            partition_by,
            order_by,
            window_frame: None,
        }
        .alias(&alias1);

        selection_0.push(window_expr);

        let dataframe = dataframe.select(selection_0.clone())?;

        // Restore original order
        let dataframe = dataframe.sort(vec![Expr::Sort {
            expr: Box::new(col("__row_number")),
            asc: true,
            nulls_first: false,
        }])?;

        // Build selection_1
        let mut selection_1: Vec<_> = input_fields
            .iter()
            .filter_map(|field| {
                if field == &alias0 || field == &alias1 {
                    None
                } else {
                    Some(col(field))
                }
            })
            .collect();

        // Now compute alias1 column by adding numeric field to alias0
        let alias0_col = col(&alias1).sub(numeric_field).alias(&alias0);
        selection_1.push(alias0_col);
        selection_1.push(col(&alias1));

        let dataframe = dataframe.select(selection_1.clone())?;

        Ok((dataframe, Default::default()))
    }
}
