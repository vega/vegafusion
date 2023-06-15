use std::sync::Arc;
use vegafusion_common::arrow::datatypes::{DataType, Field};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    Volatility,
};

/// `span(array)`
///
/// Returns the span of array: the difference between the last and first elements,
/// or array[array.length-1] - array[0].
///
/// See https://vega.github.io/vega/docs/expressions/#span
fn make_span_udf() -> ScalarUDF {
    let span_fn: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
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
                    ScalarValue::List(Some(arr), element_type) => {
                        match element_type.data_type() {
                            DataType::Float64 => {
                                if arr.is_empty() {
                                    // Span of empty array is 0
                                    ColumnarValue::Scalar(ScalarValue::from(0.0))
                                } else {
                                    let first = arr.first().unwrap().to_f64().unwrap();
                                    let last = arr.last().unwrap().to_f64().unwrap();
                                    ColumnarValue::Scalar(ScalarValue::from(last - first))
                                }
                            }
                            _ => {
                                return Err(DataFusionError::Internal(format!(
                                    "Unexpected element type for span function: {element_type}"
                                )))
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
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
    ScalarUDF::new(
        "span",
        &Signature::uniform(
            1,
            vec![
                DataType::Float64, // For null
                DataType::Null,    // For null
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &span_fn,
    )
}

lazy_static! {
    pub static ref SPAN_UDF: ScalarUDF = make_span_udf();
}
