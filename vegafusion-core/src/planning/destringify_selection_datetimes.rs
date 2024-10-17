use crate::error::Result;
use crate::planning::parse_datetime::parse_datetime;
use crate::spec::chart::{ChartSpec, MutChartVisitor};
use crate::spec::data::DataSpec;
use crate::spec::transform::formula::FormulaTransformSpec;
use crate::spec::transform::TransformSpec;
use serde_json::Value;
use std::collections::HashSet;

/// Post pre-transform transformation that detects the use of datetime strings in
/// Vega-Lite style selection "_store" datasets, and adds a transform to convert
/// them to UTC milliseconds.
pub fn destringify_selection_datetimes(spec: &mut ChartSpec) -> Result<()> {
    let mut visitor = DestringifySelectionDatetimesVisitor::new();
    spec.walk_mut(&mut visitor)?;
    Ok(())
}

struct DestringifySelectionDatetimesVisitor {}

impl DestringifySelectionDatetimesVisitor {
    pub fn new() -> Self {
        Self {}
    }
}

impl MutChartVisitor for DestringifySelectionDatetimesVisitor {
    fn visit_data(&mut self, data: &mut DataSpec, _scope: &[u32]) -> Result<()> {
        if let Some(Value::Array(values)) = &data.values {
            if let Some(Value::Object(value0)) = values.first() {
                let columns: HashSet<_> = value0.keys().cloned().collect();
                let store_columns: HashSet<_> = ["unit", "fields", "values"]
                    .iter()
                    .map(|f| f.to_string())
                    .collect();
                if store_columns == columns && data.transform.is_empty() {
                    // We have a selection store dataset with no transforms
                    // Extract the values array
                    if let Value::Array(values) = &value0["values"] {
                        if let Some(Value::Array(values)) = values.first() {
                            // Nested array, as in the case of an interval selection
                            let is_date_str: Vec<_> = values
                                .iter()
                                .map(|value| {
                                    matches!(value, Value::String(value) if parse_datetime(value, &Some(chrono_tz::UTC)).is_some())
                                })
                                .collect();

                            // Check whether we have at least one datestring to convert
                            if !is_date_str.is_empty() && is_date_str.iter().any(|v| *v) {
                                let exprs: Vec<_> = is_date_str
                                    .iter()
                                    .enumerate()
                                    .map(|(i, is_date_str)| {
                                        if *is_date_str {
                                            format!("toDate(datum.values[0][{i}])")
                                        } else {
                                            format!("datum.values[0][{i}]")
                                        }
                                    })
                                    .collect();
                                let exprs_csv = exprs.join(", ");
                                let formula_expr = format!("[[{exprs_csv}]]");
                                data.transform
                                    .push(TransformSpec::Formula(FormulaTransformSpec {
                                        expr: formula_expr,
                                        as_: "values".to_string(),
                                        extra: Default::default(),
                                    }));
                            }
                        } else {
                            // Non-nested array, as in the case of point selection
                            // Build expression strings for each element of values
                            let is_date_str: Vec<_> = values
                                .iter()
                                .map(|value| {
                                    matches!(value, Value::String(value) if parse_datetime(value, &Some(chrono_tz::UTC)).is_some())
                                })
                                .collect();

                            // Check whether we have at least one datestring to convert
                            if !is_date_str.is_empty() && is_date_str.iter().any(|v| *v) {
                                let exprs: Vec<_> = is_date_str
                                    .iter()
                                    .enumerate()
                                    .map(|(i, is_date_str)| {
                                        if *is_date_str {
                                            format!("toDate(datum.values[{i}])")
                                        } else {
                                            format!("datum.values[{i}]")
                                        }
                                    })
                                    .collect();
                                let exprs_csv = exprs.join(", ");
                                let formula_expr = format!("[{exprs_csv}]");
                                data.transform
                                    .push(TransformSpec::Formula(FormulaTransformSpec {
                                        expr: formula_expr,
                                        as_: "values".to_string(),
                                        extra: Default::default(),
                                    }));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
