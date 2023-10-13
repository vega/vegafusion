use std::sync::Arc;
use vegafusion_common::arrow::array::{Array, ArrayRef, UInt32Array};
use vegafusion_common::arrow::compute::sort_to_indices;
use vegafusion_common::arrow::datatypes::{DataType, Field, FieldRef};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{create_udaf, Accumulator, AggregateUDF, Volatility};

#[derive(Debug)]
/// The percentile_cont accumulator accumulates the raw input values
/// as `ScalarValue`s
///
/// The intermediate state is represented as a List of those scalars
pub(crate) struct PercentileContAccumulator {
    pub data_type: DataType,
    pub all_values: Vec<ScalarValue>,
    pub percentile: f64,
}

impl Accumulator for PercentileContAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>, DataFusionError> {
        let state = ScalarValue::new_list(Some(self.all_values.clone()), self.data_type.clone());
        Ok(vec![state])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let array = &values[0];

        assert_eq!(array.data_type(), &self.data_type);
        self.all_values.reserve(array.len());
        for index in 0..array.len() {
            self.all_values
                .push(ScalarValue::try_from_array(array, index)?);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        assert_eq!(states.len(), 1);

        let array = &states[0];
        assert!(matches!(array.data_type(), DataType::List(_)));
        for index in 0..array.len() {
            match ScalarValue::try_from_array(array, index)? {
                ScalarValue::List(Some(values), _) => {
                    for scalar in values {
                        if !scalar_is_non_finite(&scalar) {
                            self.all_values.push(scalar);
                        }
                    }
                }
                ScalarValue::List(None, _) => {} // skip empty state
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected state in percentile_cont. Expected DataType::List, got {v:?}"
                    )))
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, DataFusionError> {
        if !self.all_values.iter().any(|v| !v.is_null()) {
            return ScalarValue::try_from(&self.data_type);
        }

        // Create an array of all the non null values and find the
        // sorted indexes
        let array = ScalarValue::iter_to_array(
            self.all_values
                .iter()
                // ignore null values
                .filter(|v| !v.is_null())
                .cloned(),
        )?;

        // find the mid point
        let len = array.len();
        let r = (len - 1) as f64 * self.percentile;

        let limit = Some(r.ceil() as usize + 1);
        let options = None;
        let indices = sort_to_indices(&array, options, limit)?;

        let r_lower = r.floor() as usize;
        let r_upper = r.ceil() as usize;

        let result = if r_lower == r_upper {
            // Exact value found, pick that one
            scalar_at_index(&array, &indices, r_lower)?
        } else {
            // Interpolate between upper and lower values
            let s_lower = scalar_at_index(&array, &indices, r_lower)?;
            let s_upper = scalar_at_index(&array, &indices, r_upper)?;

            let f_lower = s_lower
                .to_f64()
                .map_err(|err| DataFusionError::Internal(err.to_string()))?;
            let f_upper = s_upper
                .to_f64()
                .map_err(|err| DataFusionError::Internal(err.to_string()))?;

            let result = f_lower + (f_upper - f_lower) * r.fract();
            ScalarValue::from(result)
        };

        Ok(result)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.all_values)
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

fn scalar_is_non_finite(v: &ScalarValue) -> bool {
    match v {
        ScalarValue::Float32(Some(v)) => !v.is_finite(),
        ScalarValue::Float64(Some(v)) => !v.is_finite(),
        _ => false,
    }
}

/// Given a returns `array[indicies[indicie_index]]` as a `ScalarValue`
fn scalar_at_index(
    array: &dyn Array,
    indices: &UInt32Array,
    indicies_index: usize,
) -> Result<ScalarValue, DataFusionError> {
    let array_index = indices
        .value(indicies_index)
        .try_into()
        .expect("Convert uint32 to usize");
    ScalarValue::try_from_array(array, array_index)
}

lazy_static! {
    pub static ref Q1_UDF: AggregateUDF = create_udaf(
        "q1",
        // input type
        vec![DataType::Float64],
        // the return type
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        // Accumulator factory
        Arc::new(|dtype| Ok(Box::new(PercentileContAccumulator {
            data_type: dtype.clone(),
            all_values: Default::default(),
            percentile: 0.25,
        }))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![
            DataType::List(FieldRef::new(Field::new("item", DataType::Float64, true)))
        ]),
    );

    pub static ref Q3_UDF: AggregateUDF = create_udaf(
        "q3",
        // input type
        vec![DataType::Float64],
        // the return type
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        // Accumulator factory
        Arc::new(|dtype| Ok(Box::new(PercentileContAccumulator {
            data_type: dtype.clone(),
            all_values: Default::default(),
            percentile: 0.75,
        }))),
        // This is the description of the state. `state()` must match the types here.
        Arc::new(vec![
            DataType::List(FieldRef::new(Field::new("item", DataType::Float64, true)))
        ]),
    );
}
