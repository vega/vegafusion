use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};

use vegafusion_core::error::Result;

use datafusion::logical_expr::{expr, Expr};

use std::sync::Arc;
use vegafusion_core::data::scalar::DATETIME_PREFIX;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::data::ORDER_COL;
use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
use vegafusion_rt_datafusion::expression::compiler::utils::is_numeric_datatype;
use vegafusion_rt_datafusion::expression::escape::flat_col;
use vegafusion_rt_datafusion::tokio_runtime::TOKIO_RUNTIME;
use vegafusion_rt_datafusion::transform::utils::DataFrameUtils;

const DROP_COLS: &[&str] = &[ORDER_COL, "_impute"];

#[derive(Debug, Clone)]
pub struct TablesEqualConfig {
    pub row_order: bool,
    pub tolerance: f64,
}

impl Default for TablesEqualConfig {
    fn default() -> Self {
        Self {
            row_order: true,
            tolerance: 1.0e-10,
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
        "Columns mismatch\nlhs: {:?}\n, rhs: {:?}",
        lhs_columns, rhs_columns,
    );

    // Check number of rows
    assert_eq!(
        lhs.num_rows(),
        rhs.num_rows(),
        "Number of rows mismatch\nlhs: {}, rhs: {}",
        lhs.num_rows(),
        rhs.num_rows()
    );

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
                    Some(Expr::Sort(expr::Sort {
                        expr: Box::new(flat_col(f.name())),
                        asc: false,
                        nulls_first: false,
                    }))
                }
            })
            .collect();

        let lhs_df = TOKIO_RUNTIME.block_on(lhs.to_dataframe()).unwrap();
        let rhs_df = TOKIO_RUNTIME.block_on(rhs.to_dataframe()).unwrap();

        let lhs_rb = lhs_df
            .sort(sort_exprs.clone())
            .unwrap()
            .block_flat_eval()
            .unwrap();
        let rhs_rb = rhs_df.sort(sort_exprs).unwrap().block_flat_eval().unwrap();
        (lhs_rb, rhs_rb)
    };

    let lhs_scalars = record_batch_to_scalars(&lhs_rb).unwrap();
    let rhs_scalars = record_batch_to_scalars(&rhs_rb).unwrap();

    for i in 0..lhs_scalars.len() {
        assert_scalars_almost_equals(&lhs_scalars[i], &rhs_scalars[i], config.tolerance, "row", i);
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

fn numeric_to_f64(s: &ScalarValue) -> f64 {
    match s {
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
        _ => panic!("Non-numeric value: {:?}", s),
    }
}

fn assert_scalars_almost_equals(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    tol: f64,
    name: &str,
    index: usize,
) {
    match (lhs, rhs) {
        (
            ScalarValue::Struct(Some(lhs_vals), lhs_fields),
            ScalarValue::Struct(Some(rhs_vals), rhs_fields),
        ) => {
            let lhs_map: HashMap<_, _> = lhs_fields
                .iter()
                .zip(lhs_vals.iter())
                .filter_map(|(f, val)| {
                    if DROP_COLS.contains(&f.name().as_str()) {
                        None
                    } else {
                        Some((f.name().clone(), val.clone()))
                    }
                })
                .collect();

            let rhs_map: HashMap<_, _> = rhs_fields
                .iter()
                .zip(rhs_vals.iter())
                .filter_map(|(f, val)| {
                    if DROP_COLS.contains(&f.name().as_str()) {
                        None
                    } else {
                        Some((f.name().clone(), val.clone()))
                    }
                })
                .collect();

            // Check column names
            let lhs_names: HashSet<_> = lhs_map.keys().collect();
            let rhs_names: HashSet<_> = rhs_map.keys().collect();

            assert_eq!(
                lhs_names, rhs_names,
                "Struct fields mismatch\nlhs: {:?}\n, rhs: {:?}",
                lhs_names, rhs_names,
            );

            for (key, lhs_val) in lhs_map.iter() {
                let rhs_val = &rhs_map[key];
                assert_scalars_almost_equals(lhs_val, rhs_val, tol, key, index);
            }
        }
        (_, _) => {
            // Convert TimestampMillisecond to Int64 for comparison
            let lhs = normalize_scalar(lhs);
            let rhs = normalize_scalar(rhs);

            if lhs == rhs || lhs.is_null() && rhs.is_null() {
                // Equal
            } else if is_numeric_datatype(&lhs.get_datatype())
                && is_numeric_datatype(&rhs.get_datatype())
            {
                if (lhs.is_null() || !numeric_to_f64(&lhs).is_finite())
                    && (rhs.is_null() || !numeric_to_f64(&rhs).is_finite())
                {
                    // both null, nan, inf, or -inf (which are all considered null in JSON)
                } else {
                    let lhs = numeric_to_f64(&lhs);
                    let rhs = numeric_to_f64(&rhs);
                    assert!(
                        (lhs - rhs).abs() <= tol,
                        "{} and {} are not equal to within tolerance {}, row {}, coloumn {}",
                        lhs,
                        rhs,
                        tol,
                        index,
                        name
                    )
                }
            } else {
                // This will fail
                assert_eq!(lhs, rhs, "Row {}", index)
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
        ScalarValue::Utf8(Some(s)) if s.starts_with(DATETIME_PREFIX) => {
            let v: i64 = s.strip_prefix(DATETIME_PREFIX).unwrap().parse().unwrap();
            ScalarValue::Int64(Some(v))
        }
        _ => scalar.clone(),
    }
}

pub fn assert_signals_almost_equal(lhs: Vec<ScalarValue>, rhs: Vec<ScalarValue>, tol: f64) {
    for (lhs_value, rhs_value) in lhs.iter().zip(&rhs) {
        assert_scalars_almost_equals(lhs_value, rhs_value, tol, "signal", 0)
    }
}
