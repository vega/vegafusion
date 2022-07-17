/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;

use datafusion::prelude::col;

use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext};
use vegafusion_core::proto::gen::transforms::Formula;

use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Formula {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<DataFrame>, Vec<TaskValue>)> {
        let formula_expr = compile(
            self.expr.as_ref().unwrap(),
            config,
            Some(dataframe.schema()),
        )?;

        // Rename with alias
        let formula_expr = formula_expr.alias(&self.r#as);

        // Build selections. If as field name is already present, replace it with
        // formula expression at the same position. Otherwise append formula expression
        // to the end of the selection list
        let mut selections = Vec::new();
        let mut as_field_added = false;
        for field in dataframe.schema().fields().iter() {
            if field.name() == &self.r#as {
                selections.push(formula_expr.clone());
                as_field_added = true;
            } else {
                selections.push(col(field.name()))
            }
        }
        if !as_field_added {
            selections.push(formula_expr);
        }

        // dataframe
        let result = dataframe.select(selections).with_context(|| {
            format!(
                "Formula transform failed with expression: {}",
                &self.expr.as_ref().unwrap()
            )
        })?;

        Ok((result, Default::default()))
    }
}
