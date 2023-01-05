use crate::error::Result;
use crate::planning::plan::{PlannerConfig, PlannerWarnings};
use crate::spec::chart::{ChartSpec, ChartVisitor};
use crate::spec::data::{DataSpec, DependencyNodeSupported};
use crate::task_graph::scope::TaskScope;

pub fn add_unsupported_data_warnings(
    chart_spec: &ChartSpec,
    config: &PlannerConfig,
    warnings: &mut Vec<PlannerWarnings>,
) -> Result<()> {
    let task_scope = chart_spec.to_task_scope()?;
    let mut visitor = CollectUnsupportedDatasetsWarningsVisitor::new(config, &task_scope, warnings);
    chart_spec.walk(&mut visitor)?;
    Ok(())
}

/// Visitor to collect unsupported datasets
#[derive(Debug)]
pub struct CollectUnsupportedDatasetsWarningsVisitor<'a> {
    pub planner_config: &'a PlannerConfig,
    pub task_scope: &'a TaskScope,
    pub warnings: &'a mut Vec<PlannerWarnings>,
}

impl<'a> CollectUnsupportedDatasetsWarningsVisitor<'a> {
    pub fn new(
        planner_config: &'a PlannerConfig,
        task_scope: &'a TaskScope,
        warnings: &'a mut Vec<PlannerWarnings>,
    ) -> Self {
        Self {
            planner_config,
            task_scope,
            warnings,
        }
    }
}

impl<'a> ChartVisitor for CollectUnsupportedDatasetsWarningsVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let data_suported = data.supported(self.planner_config, self.task_scope, scope);
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
