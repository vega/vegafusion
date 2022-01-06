/*
 * VegaFusion
 * Copyright (C) 2022 Jon Mease
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public
 * License along with this program.
 * If not, see http://www.gnu.org/licenses/.
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
