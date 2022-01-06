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
