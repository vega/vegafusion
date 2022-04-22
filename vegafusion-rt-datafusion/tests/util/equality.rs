/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;

use vegafusion_core::error::Result;

use datafusion::logical_plan::{col, Expr};

use std::sync::Arc;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_rt_datafusion::data::table::VegaFusionTableUtils;
use vegafusion_rt_datafusion::expression::compiler::utils::is_numeric_datatype;
use vegafusion_rt_datafusion::transform::utils::DataFrameUtils;

#[derive(Debug, Clone)]
pub struct TablesEqualConfig {
    pub row_order: bool,
    pub tolerance: f64,
}

impl Default for TablesEqualConfig {
    fn default() -> Self {
        Self {
            row_order: true,
            tolerance: 1.0e-12,
        }
    }
}

pub fn assert_tables_equal(
    lhs: &VegaFusionTable,
    rhs: &VegaFusionTable,
    config: &TablesEqualConfig,
) {
    // Check column names
    let lhs_columns: Vec<_> = lhs
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let rhs_columns: Vec<_> = rhs
        .schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
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
        // Sort by all columns
        let sort_exprs: Vec<_> = lhs
            .schema
            .fields()
            .iter()
            .map(|f| Expr::Sort {
                expr: Box::new(col(f.name())),
                asc: false,
                nulls_first: false,
            })
            .collect();

        let lhs_df = lhs.to_dataframe().unwrap();
        let rhs_df = rhs.to_dataframe().unwrap();

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
        assert_scalars_almost_equals(&lhs_scalars[i], &rhs_scalars[i], config.tolerance);
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

fn assert_scalars_almost_equals(lhs: &ScalarValue, rhs: &ScalarValue, tol: f64) {
    match (lhs, rhs) {
        (
            ScalarValue::Struct(Some(lhs_vals), lhs_fields),
            ScalarValue::Struct(Some(rhs_vals), rhs_fields),
        ) => {
            // Check column names
            let lhs_field_names: Vec<_> = lhs_fields.iter().map(|f| f.name().clone()).collect();
            let rhs_field_names: Vec<_> = rhs_fields.iter().map(|f| f.name().clone()).collect();
            assert_eq!(
                lhs_field_names, rhs_field_names,
                "Struct fields mismatch\nlhs: {:?}\n, rhs: {:?}",
                lhs_field_names, rhs_field_names,
            );

            for i in 0..lhs_vals.len() {
                assert_scalars_almost_equals(&lhs_vals[i], &rhs_vals[i], tol);
            }
        }
        (_, _) => {
            if lhs == rhs || lhs.is_null() && rhs.is_null() {
                // Equal
            } else if is_numeric_datatype(&lhs.get_datatype())
                && is_numeric_datatype(&rhs.get_datatype())
            {
                if (lhs.is_null() || !numeric_to_f64(lhs).is_finite())
                    && (rhs.is_null() || !numeric_to_f64(rhs).is_finite())
                {
                    // both null, nan, inf, or -inf (which are all considered null in JSON)
                } else {
                    let lhs = numeric_to_f64(lhs);
                    let rhs = numeric_to_f64(rhs);
                    assert!(
                        (lhs - rhs).abs() <= tol,
                        "{} and {} are not equal to within tolerance {}",
                        lhs,
                        rhs,
                        tol
                    )
                }
            } else {
                // This will fail
                assert_eq!(lhs, rhs)
            }
        }
    }
}

pub fn assert_signals_almost_equal(lhs: Vec<ScalarValue>, rhs: Vec<ScalarValue>, tol: f64) {
    for (lhs_value, rhs_value) in lhs.iter().zip(&rhs) {
        assert_scalars_almost_equals(lhs_value, rhs_value, tol)
    }
}
