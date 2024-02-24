use std::any::Any;
use std::sync::Arc;
use vegafusion_common::arrow::array::Array;
use vegafusion_common::arrow::array::ListArray;
use vegafusion_common::arrow::datatypes::{DataType, Field};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

/// `span(array)`
///
/// Returns the span of array: the difference between the last and first elements,
/// or array[array.length-1] - array[0].
///
/// See https://vega.github.io/vega/docs/expressions/#span
#[derive(Debug, Clone)]
pub struct SpanUDF {
    signature: Signature,
}

impl Default for SpanUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl SpanUDF {
    pub fn new() -> Self {
        let signature = Signature::uniform(
            1,
            vec![
                DataType::Float64, // For null
                DataType::Null,    // For null
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for SpanUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::Null => ColumnarValue::Scalar(ScalarValue::from(0.0)),
                    ScalarValue::Float64(_) => {
                        // Span of scalar (including null) is 0
                        ColumnarValue::Scalar(ScalarValue::from(0.0))
                    }
                    ScalarValue::List(arr) => {
                        // Unwrap single element ListArray
                        let arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
                        let arr = arr.value(0);
                        if arr.is_empty() {
                            ColumnarValue::Scalar(ScalarValue::from(0.0))
                        } else {
                            match arr.data_type() {
                                DataType::Float64 => {
                                    let first = ScalarValue::try_from_array(&arr, 0)
                                        .unwrap()
                                        .to_f64()
                                        .unwrap();
                                    let last = ScalarValue::try_from_array(&arr, arr.len() - 1)
                                        .unwrap()
                                        .to_f64()
                                        .unwrap();
                                    ColumnarValue::Scalar(ScalarValue::from(last - first))
                                }
                                _ => {
                                    return Err(DataFusionError::Internal(format!(
                                        "Unexpected element type for span function: {}",
                                        arr.data_type()
                                    )))
                                }
                            }
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unexpected type passed to span: {value}"
                        )))
                    }
                }
            }
            ColumnarValue::Array(_array) => {
                todo!("Span on column not yet implemented")
            }
        })
    }
}
