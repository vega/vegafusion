use ordered_float::NotNan;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use vegafusion_common::arrow::array::{
    new_null_array, Array, ArrayRef, Float64Array, Int32Array, StringArray,
};
use vegafusion_common::arrow::compute::cast;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::data::scalar::{ArrayRefHelpers, ScalarValueHelpers};
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use vegafusion_common::datatypes::{is_numeric_datatype, is_string_datatype};

/// `indexof(array, value)`
///
/// Returns the first index of value in the input array.
///
/// See https://vega.github.io/vega/docs/expressions/#indexof
/// and https://vega.github.io/vega/docs/expressions/#string_indexof
#[derive(Debug, Clone)]
pub struct IndexOfUDF {
    signature: Signature,
}

impl Default for IndexOfUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexOfUDF {
    pub fn new() -> Self {
        let signature = Signature::any(2, Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for IndexOfUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "indexof"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Int32)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // Signature ensures there is a single argument
        let (array, array_dtype) = match &args[0] {
            ColumnarValue::Scalar(ScalarValue::List(array)) => {
                let scalar_array = array.value(0).to_scalar_vec()?;
                (scalar_array, array.value(0).data_type().clone())
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "index of array argument may not be a ColumnarValue::Array".to_string(),
                ))
            }
        };

        let arg = &args[1];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                let value_dtype = value.data_type();
                if is_numeric_datatype(&value_dtype) && is_numeric_datatype(&array_dtype) {
                    let indices = build_notnan_index_map(array.as_slice());
                    if let Ok(value) = value.to_f64() {
                        match NotNan::new(value) {
                            Ok(v) => {
                                let index = indices.get(&v).cloned().unwrap_or(-1);
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(index)))
                            }
                            Err(_) => {
                                // nan is always not found
                                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1)))
                            }
                        }
                    } else {
                        // non numeric (e.g. NULL) always not found
                        ColumnarValue::Scalar(ScalarValue::Int32(Some(-1)))
                    }
                } else if is_string_datatype(&value_dtype) && is_string_datatype(&array_dtype) {
                    let indices = array
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| (v.to_scalar_string().unwrap(), i as i32))
                        .collect::<HashMap<_, _>>();

                    let value_string = value.to_scalar_string().unwrap();
                    let index = indices.get(&value_string).cloned().unwrap_or(-1);
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(index)))
                } else {
                    // null
                    ColumnarValue::Scalar(ScalarValue::try_from(&DataType::Int32).unwrap())
                }
            }
            ColumnarValue::Array(value) => {
                let value_dtype = value.data_type().clone();
                if is_numeric_datatype(&value_dtype) && is_numeric_datatype(&array_dtype) {
                    let indices = build_notnan_index_map(array.as_slice());
                    let value_f64 = cast(value, &DataType::Float64)?;
                    let value_f64 = value_f64.as_any().downcast_ref::<Float64Array>().unwrap();

                    let mut indices_builder = Int32Array::builder(value_f64.len());
                    for v in value_f64 {
                        indices_builder.append_value(match v {
                            Some(v) => match NotNan::new(v) {
                                Ok(v) => indices.get(&v).cloned().unwrap_or(-1),
                                Err(_) => -1,
                            },
                            None => -1,
                        })
                    }
                    ColumnarValue::Array(Arc::new(indices_builder.finish()) as ArrayRef)
                } else if is_string_datatype(&value_dtype) && is_string_datatype(&array_dtype) {
                    let indices = array
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| (v.to_scalar_string().unwrap(), i as i32))
                        .collect::<HashMap<_, _>>();

                    let value = value.as_any().downcast_ref::<StringArray>().unwrap();

                    let mut indices_builder = Int32Array::builder(value.len());
                    for s in value {
                        indices_builder.append_value(match s {
                            Some(s) => indices.get(s).cloned().unwrap_or(-1),
                            None => -1,
                        })
                    }
                    ColumnarValue::Array(Arc::new(indices_builder.finish()) as ArrayRef)
                } else {
                    // Array of i32 nulls
                    ColumnarValue::Array(new_null_array(&DataType::Int32, array.len()))
                }
            }
        })
    }
}

fn build_notnan_index_map(array: &[ScalarValue]) -> HashMap<NotNan<f64>, i32> {
    array
        .iter()
        .enumerate()
        .filter_map(|(i, v)| {
            if let Ok(v) = v.to_f64() {
                if let Ok(v) = NotNan::new(v) {
                    return Some((v, i as i32));
                }
            }
            None
        })
        .collect::<HashMap<_, _>>()
}
