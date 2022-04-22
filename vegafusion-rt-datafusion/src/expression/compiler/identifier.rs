/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::config::CompilationConfig;
use datafusion::logical_plan::{lit, Expr};
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::expression::Identifier;

pub fn compile_identifier(node: &Identifier, config: &CompilationConfig) -> Result<Expr> {
    let value = if let Some(value) = config.signal_scope.get(&node.name) {
        value.clone()
    } else if let Some(value) = config.constants.get(&node.name) {
        value.clone()
    } else {
        return Err(VegaFusionError::compilation(&format!(
            "No signal named {} in evaluation scope",
            node.name
        )));
    };

    Ok(lit(value))
}
