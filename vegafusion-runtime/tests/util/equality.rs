use std::collections::{HashMap, HashSet};

use datafusion_common::ScalarValue;
use datafusion_expr::expr;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, StructArray};
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::scalar::DATETIME_PREFIX;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::datatypes::is_numeric_datatype;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_runtime::data::util::{DataFrameUtils, SessionContextUtils};
use vegafusion_runtime::datafusion::context::make_datafusion_context;
use vegafusion_runtime::tokio_runtime::TOKIO_RUNTIME;

const DROP_COLS: &[&str] = &[ORDER_COL, "_impute"];

#[derive(Debug, Clone)]
pub struct TablesEqualConfig {
    pub row_order: bool,
    pub tolerance: f64,
    pub null_matches_zero: bool,
}

impl Default for TablesEqualConfig {
    fn default() -> Self {
        Self {
            row_order: true,
            tolerance: 1.0e-10,
            null_matches_zero: false,
        }
    }
}

pub fn assert_tables_equal(
    lhs: &VegaFusionTable,
    rhs: &VegaFusionTable,
    config: &TablesEqualConfig,
) {
    if lhs.num_rows() == 0 && rhs.num_rows() == 0 {
        // Tables are both empty, don't try to compare schema
        return;
    }

    // Check column names (filtering out order col)
    let lhs_columns: HashSet<_> = lhs
        .schema
        .fields()
        .iter()
        .filter_map(|f| {
            if DROP_COLS.contains(&f.name().as_str()) {
                None
            } else {
                Some(f.name().clone())
            }
        })
        .collect();
    let rhs_columns: HashSet<_> = rhs
        .schema
        .fields()
        .iter()
        .filter_map(|f| {
            if DROP_COLS.contains(&f.name().as_str()) {
                None
            } else {
                Some(f.name().clone())
            }
        })
        .collect();
    assert_eq!(
        lhs_columns, rhs_columns,
        "Columns mismatch\nlhs: {lhs_columns:?}\n, rhs: {rhs_columns:?}",
    );

    // Check number of rows
    assert_eq!(
        lhs.num_rows(),
        rhs.num_rows(),
        "Number of rows mismatch\nlhs: {}, rhs: {}",
        lhs.num_rows(),
        rhs.num_rows()
    );

    let ctx = Arc::new(make_datafusion_context());

    // Flatten to single record batch
    let (lhs_rb, rhs_rb) = if config.row_order {
        let lhs_rb = lhs.to_record_batch().unwrap();
        let rhs_rb = rhs.to_record_batch().unwrap();
        (lhs_rb, rhs_rb)
    } else {
        // Sort by all columns except ORDER_COL
        let sort_exprs: Vec<_> = lhs
            .schema
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == ORDER_COL {
                    None
                } else {
                    Some(expr::Sort {
                        expr: flat_col(f.name()),
                        asc: false,
                        nulls_first: false,
                    })
                }
            })
            .collect();

        let lhs_df = TOKIO_RUNTIME
            .block_on(ctx.vegafusion_table(lhs.clone()))
            .unwrap();
        let rhs_df = TOKIO_RUNTIME
            .block_on(ctx.vegafusion_table(rhs.clone()))
            .unwrap();

        let lhs_rb = TOKIO_RUNTIME.block_on(async {
            lhs_df
                .sort(sort_exprs.clone())
                .unwrap()
                .collect_flat()
                .await
                .unwrap()
        });

        let rhs_rb = TOKIO_RUNTIME.block_on(async {
            rhs_df
                .sort(sort_exprs.clone())
                .unwrap()
                .collect_flat()
                .await
                .unwrap()
        });

        (lhs_rb, rhs_rb)
    };

    let lhs_scalars = record_batch_to_scalars(&lhs_rb).unwrap();
    let rhs_scalars = record_batch_to_scalars(&rhs_rb).unwrap();

    for i in 0..lhs_scalars.len() {
        assert_scalars_almost_equals(
            &lhs_scalars[i],
            &rhs_scalars[i],
            config.tolerance,
            "row",
            i,
            config.null_matches_zero,
        );
    }
}

fn record_batch_to_scalars(rb: &RecordBatch) -> Result<Vec<ScalarValue>> {
    let struct_array = Arc::new(StructArray::from(rb.clone())) as ArrayRef;
    let mut result: Vec<ScalarValue> = Vec::new();
    for i in 0..rb.num_rows() {
        result.push(ScalarValue::try_from_array(&struct_array, i)?)
    }
    Ok(result)
}

fn numeric_to_f64(s: &ScalarValue) -> Result<f64> {
    Ok(match s {
        ScalarValue::Float32(Some(v)) => *v as f64,
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Int8(Some(v)) => *v as f64,
        ScalarValue::Int16(Some(v)) => *v as f64,
        ScalarValue::Int32(Some(v)) => *v as f64,
        ScalarValue::Int64(Some(v)) => *v as f64,
        ScalarValue::UInt8(Some(v)) => *v as f64,
        ScalarValue::UInt16(Some(v)) => *v as f64,
        ScalarValue::UInt32(Some(v)) => *v as f64,
        ScalarValue::UInt64(Some(v)) => *v as f64,
        _ => return Err(VegaFusionError::internal("non-numeric value")),
    })
}

pub fn assert_scalars_almost_equals(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    tol: f64,
    name: &str,
    index: usize,
    null_matches_zero: bool,
) {
    match (lhs, rhs) {
        (ScalarValue::Struct(lhs_sa), ScalarValue::Struct(rhs_sa)) => {
            let lhs_map: HashMap<_, _> = lhs_sa
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(field_ind, field)| {
                    if DROP_COLS.contains(&field.name().as_str()) {
                        None
                    } else {
                        let val = ScalarValue::try_from_array(lhs_sa.column(field_ind), 0).unwrap();
                        Some((field.name().clone(), val))
                    }
                })
                .collect();

            let rhs_map: HashMap<_, _> = rhs_sa
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(field_ind, field)| {
                    if DROP_COLS.contains(&field.name().as_str()) {
                        None
                    } else {
                        let val = ScalarValue::try_from_array(rhs_sa.column(field_ind), 0).unwrap();
                        Some((field.name().clone(), val))
                    }
                })
                .collect();

            // Check column names
            let lhs_names: HashSet<_> = lhs_map.keys().collect();
            let rhs_names: HashSet<_> = rhs_map.keys().collect();

            assert_eq!(
                lhs_names, rhs_names,
                "Struct fields mismatch\nlhs: {lhs_names:?}\n, rhs: {rhs_names:?}",
            );

            for (key, lhs_val) in lhs_map.iter() {
                let rhs_val = &rhs_map[key];
                assert_scalars_almost_equals(lhs_val, rhs_val, tol, key, index, null_matches_zero);
            }
        }
        (_, _) => {
            // Convert TimestampMillisecond to Int64 for comparison
            let lhs = normalize_scalar(lhs);
            let rhs = normalize_scalar(rhs);

            if lhs == rhs || lhs.is_null() && rhs.is_null() {
                // Equal
            } else if is_numeric_datatype(&lhs.data_type()) && is_numeric_datatype(&rhs.data_type())
            {
                let lhs_finite = numeric_to_f64(&lhs).map(|v| v.is_finite()).unwrap_or(false);
                let rhs_finite = numeric_to_f64(&rhs).map(|v| v.is_finite()).unwrap_or(false);
                if !lhs_finite && !rhs_finite {
                    // both non-finite or null, consider equal
                    return;
                } else {
                    match (numeric_to_f64(&lhs), numeric_to_f64(&rhs)) {
                        (Ok(lhs), Ok(rhs)) => {
                            assert!(
                                (lhs - rhs).abs() <= tol,
                                "{lhs} and {rhs} are not equal to within tolerance {tol}, row {index}, coloumn {name}"
                            )
                        }
                        (Ok(0.0), Err(_)) | (Err(_), Ok(0.0)) if null_matches_zero => {
                            // OK
                        }
                        _ => {
                            panic!("{lhs:?} and {rhs:?} are not equal, row {index}, coloumn {name}")
                        }
                    }
                }
            } else {
                // This will fail
                assert_eq!(lhs, rhs, "Row {index}")
            }
        }
    }
}

pub fn normalize_scalar(scalar: &ScalarValue) -> ScalarValue {
    flip_negative_zero(timestamp_to_int(scalar))
}

fn flip_negative_zero(scalar: ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::Float64(Some(v)) if v.abs() == 0.0 => ScalarValue::Float64(Some(0.0)),
        _ => scalar,
    }
}

fn timestamp_to_int(scalar: &ScalarValue) -> ScalarValue {
    match scalar {
        ScalarValue::TimestampMillisecond(Some(v), _) => ScalarValue::Int64(Some(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => ScalarValue::Int64(Some(*v / 1000000)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => ScalarValue::Int64(Some(*v / 1000)),
        ScalarValue::TimestampSecond(Some(v), _) => ScalarValue::Int64(Some(*v * 1000)),
        ScalarValue::Utf8(Some(s)) if s.starts_with(DATETIME_PREFIX) => {
            let v: i64 = s.strip_prefix(DATETIME_PREFIX).unwrap().parse().unwrap();
            ScalarValue::Int64(Some(v))
        }
        _ => scalar.clone(),
    }
}

pub fn assert_signals_almost_equal(lhs: Vec<ScalarValue>, rhs: Vec<ScalarValue>, tol: f64) {
    for (lhs_value, rhs_value) in lhs.iter().zip(&rhs) {
        assert_scalars_almost_equals(lhs_value, rhs_value, tol, "signal", 0, false)
    }
}
