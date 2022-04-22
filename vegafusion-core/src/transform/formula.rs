/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::expression::parser::parse;
use crate::proto::gen::transforms::Formula;
use crate::spec::transform::formula::FormulaTransformSpec;
use crate::transform::TransformDependencies;

use crate::task_graph::task::InputVariable;

impl Formula {
    pub fn try_new(spec: &FormulaTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        Ok(Self {
            expr: Some(expr),
            r#as: spec.as_.clone(),
        })
    }
}

impl TransformDependencies for Formula {
    fn input_vars(&self) -> Vec<InputVariable> {
        self.expr.as_ref().unwrap().input_vars()
    }
}
