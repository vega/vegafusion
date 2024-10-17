use super::{
    destringify_selection_datetimes::destringify_selection_datetimes,
    plan::SpecPlan,
    watch::{ExportUpdateArrow, ExportUpdateNamespace},
};
use crate::{
    proto::gen::{
        pretransform::{
            pre_transform_spec_warning::WarningType, PlannerWarning,
            PreTransformBrokenInteractivityWarning, PreTransformRowLimitWarning,
            PreTransformSpecWarning, PreTransformUnsupportedWarning,
        },
        tasks::Variable,
    },
    spec::{chart::ChartSpec, values::MissingNullOrValue},
};
use serde_json::Value;
use vegafusion_common::error::{Result, VegaFusionError};

pub fn apply_pre_transform_datasets(
    input_spec: &ChartSpec,
    plan: &SpecPlan,
    init: Vec<ExportUpdateArrow>,
    row_limit: Option<u32>,
) -> Result<(ChartSpec, Vec<PreTransformSpecWarning>)> {
    // Update client spec with server values
    let mut spec = plan.client_spec.clone();
    let mut limited_datasets: Vec<Variable> = Vec::new();
    for export_update in init {
        let scope = export_update.scope.clone();
        let name = export_update.name.as_str();
        let export_update = export_update.to_json()?;
        match export_update.namespace {
            ExportUpdateNamespace::Signal => {
                let signal = spec.get_nested_signal_mut(&scope, name)?;
                signal.value = MissingNullOrValue::Value(export_update.value);
            }
            ExportUpdateNamespace::Data => {
                let data = spec.get_nested_data_mut(&scope, name)?;

                // If the input dataset includes inline values and no transforms,
                // copy the input JSON directly to avoid the case where round-tripping
                // through Arrow homogenizes mixed type arrays.
                // E.g. round tripping may turn [1, "two"] into ["1", "two"]
                let input_values = input_spec
                    .get_nested_data(&scope, name)
                    .ok()
                    .and_then(|data| {
                        if data.transform.is_empty() {
                            data.values.clone()
                        } else {
                            None
                        }
                    });
                let value = if let Some(input_values) = input_values {
                    input_values
                } else if let Value::Array(values) = &export_update.value {
                    if let Some(row_limit) = row_limit {
                        let row_limit = row_limit as usize;
                        if values.len() > row_limit {
                            limited_datasets.push(export_update.to_scoped_var().0);
                            Value::Array(Vec::from(&values[..row_limit]))
                        } else {
                            Value::Array(values.clone())
                        }
                    } else {
                        Value::Array(values.clone())
                    }
                } else {
                    return Err(VegaFusionError::internal(
                        "Expected Data value to be an Array",
                    ));
                };

                // Set inline value
                // Other properties are cleared by planning process so we don't alter them here
                data.values = Some(value);
            }
        }
    }

    // Destringify datetime strings in selection store datasets
    destringify_selection_datetimes(&mut spec)?;

    // Build warnings
    let mut warnings: Vec<PreTransformSpecWarning> = Vec::new();

    // Add unsupported warning (
    if plan.comm_plan.server_to_client.is_empty() {
        warnings.push(PreTransformSpecWarning {
            warning_type: Some(WarningType::Unsupported(PreTransformUnsupportedWarning {})),
        });
    }

    // Add Row Limit warning
    if !limited_datasets.is_empty() {
        warnings.push(PreTransformSpecWarning {
            warning_type: Some(WarningType::RowLimit(PreTransformRowLimitWarning {
                datasets: limited_datasets,
            })),
        });
    }

    // Add Broken Interactivity warning
    if !plan.comm_plan.client_to_server.is_empty() {
        let vars: Vec<_> = plan
            .comm_plan
            .client_to_server
            .iter()
            .map(|var| var.0.clone())
            .collect();
        warnings.push(PreTransformSpecWarning {
            warning_type: Some(WarningType::BrokenInteractivity(
                PreTransformBrokenInteractivityWarning { vars },
            )),
        });
    }

    // Add planner warnings
    for planner_warning in &plan.warnings {
        warnings.push(PreTransformSpecWarning {
            warning_type: Some(WarningType::Planner(PlannerWarning {
                message: planner_warning.message(),
            })),
        });
    }

    Ok((spec, warnings))
}
