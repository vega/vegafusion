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
use crate::proto::gen::tasks::Variable;
use crate::proto::gen::transforms::Extent;
use crate::spec::transform::extent::ExtentTransformSpec;
use crate::transform::TransformDependencies;

impl Extent {
    pub fn new(spec: &ExtentTransformSpec) -> Self {
        Self {
            field: spec.field.clone(),
            signal: spec.signal.clone(),
        }
    }
}

impl TransformDependencies for Extent {
    fn output_vars(&self) -> Vec<Variable> {
        self.signal
            .clone()
            .iter()
            .map(|s| Variable::new_signal(s))
            .collect()
    }
}
