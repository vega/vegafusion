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
use crate::proto::gen::transforms::Filter;
use crate::spec::transform::filter::FilterTransformSpec;
use crate::transform::TransformDependencies;

use crate::task_graph::task::InputVariable;

impl Filter {
    pub fn try_new(spec: &FilterTransformSpec) -> Result<Self> {
        let expr = parse(&spec.expr)?;
        Ok(Self { expr: Some(expr) })
    }
}

impl TransformDependencies for Filter {
    fn input_vars(&self) -> Vec<InputVariable> {
        self.expr.as_ref().unwrap().input_vars()
    }
}
