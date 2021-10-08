use datafusion::arrow::array::{ArrayRef, StructArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::scalar::ScalarValue;
use std::collections::{HashMap, HashSet};
use vega_fusion::data::table::VegaFusionTable;
use vega_fusion::error::Result;

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TablesEqualConfig {
    pub row_order: bool,
}

impl Default for TablesEqualConfig {
    fn default() -> Self {
        Self { row_order: true }
    }
}

pub fn assert_tables_equal(
    lhs: &VegaFusionTable,
    rhs: &VegaFusionTable,
    config: &TablesEqualConfig,
) {
    // Check schema
    assert_eq!(
        lhs.schema, rhs.schema,
        "Schema mismatch\nlhs: {}\n, rhs: {}",
        lhs.schema, rhs.schema
    );

    // Flatten to single record batch
    let lhs_rb = lhs.to_record_batch().unwrap();
    let rhs_rb = rhs.to_record_batch().unwrap();

    let lhs_scalars = record_batch_to_scalars(&lhs_rb).unwrap();
    let rhs_scalars = record_batch_to_scalars(&rhs_rb).unwrap();

    // Check number of rows
    assert_eq!(
        lhs_rb.num_rows(),
        rhs_rb.num_rows(),
        "Number of rows mismatch\nlhs: {}, rhs: {}",
        lhs_rb.num_rows(),
        rhs_rb.num_rows()
    );

    // Check rows
    if config.row_order {
        for i in 0..lhs_scalars.len() {
            assert_eq!(
                lhs_scalars[i], rhs_scalars[i],
                "Mismatch in row {}\n lhs: {}, rhs: {}",
                i, lhs_scalars[i], rhs_scalars[i]
            )
        }
    } else {
        // Collect count of each unique row
        let mut lhs_counts: HashMap<ScalarValue, usize> = Default::default();
        for row in lhs_scalars {
            let entry = lhs_counts.entry(row).or_insert(0);
            *entry += 1;
        }

        let mut rhs_counts: HashMap<ScalarValue, usize> = Default::default();
        for row in rhs_scalars {
            let entry = rhs_counts.entry(row).or_insert(0);
            *entry += 1;
        }

        // Get set of unique rows
        let lhs_key_set: HashSet<_> = lhs_counts.keys().collect();
        let rhs_key_set: HashSet<_> = rhs_counts.keys().collect();

        // Check for rows present in one side and not the other
        let lhs_diff_rhs: Vec<_> = lhs_key_set.difference(&rhs_key_set).collect();
        assert!(
            lhs_diff_rhs.is_empty(),
            "Found rows in LHS that are not present in RHS\n{:#?}",
            lhs_diff_rhs
        );

        let rhs_diff_lhs: Vec<_> = rhs_key_set.difference(&lhs_key_set).collect();
        assert!(
            rhs_diff_lhs.is_empty(),
            "Found rows in RHS that are not present in LHS\n{:#?}",
            rhs_diff_lhs
        );

        // Check for difference in row counts
        for (row, lhs_count) in &lhs_counts {
            let rhs_count = rhs_counts.get(row).unwrap();
            assert_eq!(
                lhs_count, rhs_count,
                "Found row that appears {} times in LHS and {} times in RHS\n{}",
                lhs_count, rhs_count, row
            )
        }
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
