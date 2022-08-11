/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::planning::plan::{PlannerConfig, PlannerWarnings};
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};

pub fn add_unsupported_data_warnings(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
    warnings: &mut Vec<PlannerWarnings>,
) -> Result<()> {
    let mut visitor =
        CollectUnsupportedDatasetsWarningsVisitor::new(config.extract_inline_data, warnings);
    chart_spec.walk(&mut visitor)?;
    Ok(())
}

/// Visitor to collect unsupported datasets
#[derive(Debug)]
pub struct CollectUnsupportedDatasetsWarningsVisitor<'a> {
    pub extract_inline_data: bool,
    pub warnings: &'a mut Vec<PlannerWarnings>,
}

impl<'a> CollectUnsupportedDatasetsWarningsVisitor<'a> {
    pub fn new(extract_inline_data: bool, warnings: &'a mut Vec<PlannerWarnings>) -> Self {
        Self {
            extract_inline_data,
            warnings,
        }
    }
}

impl<'a> ChartVisitor for CollectUnsupportedDatasetsWarningsVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let data_suported = data.supported(self.extract_inline_data);
        if !matches!(data_suported, DependencyNodeSupported::Supported) {
            let message = if scope.is_empty() {
                format!(
                    "Some transforms applied to the '{}' dataset are not yet supported on the server",
                    data.name
                )
            } else {
                format!(
                    "Some transforms applied to the '{}' dataset with scope {:?} are not yet supported on the server",
                    data.name,
                    scope
                )
            };
            self.warnings
                .push(PlannerWarnings::UnsupportedTransforms(message));
        }

        Ok(())
    }
}
