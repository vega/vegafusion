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

        let mut selections: Vec<_> = dataframe
            .schema()
            .fields()
            .iter()
            .filter_map(|f| match f.field().name() {
                s if s != &self.r#as => Some(col(s)),
                _ => None,
            })
            .collect();

        selections.push(formula_expr);

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
