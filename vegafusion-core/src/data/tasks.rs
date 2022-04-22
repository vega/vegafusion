/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::proto::gen::tasks::data_url_task::Url;
use crate::proto::gen::tasks::{DataSourceTask, DataUrlTask, DataValuesTask, SignalTask, Variable};
use crate::task_graph::task::{InputVariable, TaskDependencies};
use crate::transform::TransformDependencies;
use itertools::sorted;
use std::collections::HashSet;

impl TaskDependencies for DataValuesTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        sorted(vars).collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for DataUrlTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Collect input vars from URL signal
        if let Url::Expr(expr) = self.url.as_ref().unwrap() {
            vars.extend(expr.input_vars());
        }

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        sorted(vars).collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for DataSourceTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars: HashSet<InputVariable> = Default::default();

        // Add input vars from source
        vars.insert(InputVariable {
            var: Variable::new_data(&self.source),
            propagate: true,
        });

        // Collect transform input vars
        if let Some(pipeline) = self.pipeline.as_ref() {
            vars.extend(pipeline.input_vars());
        }

        // Return variables sorted for determinism
        sorted(vars).collect()
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.pipeline
            .as_ref()
            .iter()
            .flat_map(|p| p.output_vars())
            .collect()
    }
}

impl TaskDependencies for SignalTask {
    fn input_vars(&self) -> Vec<InputVariable> {
        let expr = self.expr.as_ref().unwrap();
        expr.input_vars()
    }
}
