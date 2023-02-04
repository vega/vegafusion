use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_common::arrow::array::{new_null_array, Array, Float64Array, ListArray};
use vegafusion_common::arrow::compute::kernels;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::ScalarValue;
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
    Volatility,
};

/// `length(array)`
///
/// Returns the length of the input array or string
///
/// See https://vega.github.io/vega/docs/expressions/#length
fn make_length_udf() -> ScalarUDF {
    let length_fn: ScalarFunctionImplementation = Arc::new(|args: &[ColumnarValue]| {
        // Signature ensures there is a single argument
        let arg = &args[0];
        Ok(match arg {
            ColumnarValue::Scalar(value) => {
                match value {
                    ScalarValue::List(Some(arr), _) => {
                        ColumnarValue::Scalar(ScalarValue::from(arr.len() as f64))
                    }
                    ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                        ColumnarValue::Scalar(ScalarValue::from(s.len() as f64))
                    }
                    _ => {
                        // Scalar f64 null
                        ColumnarValue::Scalar(ScalarValue::try_from(&DataType::Float64).unwrap())
                    }
                }
            }
            ColumnarValue::Array(array) => {
                match array.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        // String length
                        ColumnarValue::Array(
                            kernels::cast(
                                &kernels::length::length(array.as_ref()).unwrap(),
                                &DataType::Float64,
                            )
                            .unwrap(),
                        )
                    }
                    DataType::FixedSizeList(_, n) => {
                        // Use scalar length
                        ColumnarValue::Scalar(ScalarValue::from(*n as f64))
                    }
                    DataType::List(_) => {
                        let array = array.as_any().downcast_ref::<ListArray>().unwrap();
                        let offsets = array.value_offsets();
                        let mut length_builder = Float64Array::builder(array.len());

                        for i in 0..array.len() {
                            length_builder.append_value((offsets[i + 1] - offsets[i]) as f64);
                        }

                        ColumnarValue::Array(Arc::new(length_builder.finish()))
                    }
                    _ => {
                        // Array of f64
                        ColumnarValue::Array(new_null_array(&DataType::Float64, array.len()))
                    }
                }
            }
        })
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
    ScalarUDF::new(
        "len",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &length_fn,
    )
}

lazy_static! {
    pub static ref LENGTH_UDF: ScalarUDF = make_length_udf();
}
