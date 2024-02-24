use datafusion_physical_expr::udf::ScalarUDF;
use std::any::Any;
use std::sync::Arc;
use vegafusion_common::arrow::array::{BooleanArray, Float32Array, Float64Array};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::datafusion_common::{DataFusionError, ScalarValue};
use vegafusion_common::datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

/// `isFinite(value)`
///
/// Returns true if value is a finite number.
///
/// See: https://vega.github.io/vega/docs/expressions/#isFinite
#[derive(Debug, Clone)]
pub struct IsFiniteUDF {
    signature: Signature,
}

impl Default for IsFiniteUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl IsFiniteUDF {
    pub fn new() -> Self {
        let signature = Signature::any(1, Volatility::Immutable);
        Self { signature }
    }
}

impl ScalarUDFImpl for IsFiniteUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "isfinite"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // Signature ensures there is a single argument
        match &args[0] {
            ColumnarValue::Scalar(arg) => {
                let res = match arg {
                    ScalarValue::Float32(Some(v)) => v.is_finite(),
                    ScalarValue::Float64(Some(v)) => v.is_finite(),
                    _ => true,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::from(res)))
            }
            ColumnarValue::Array(arg) => {
                let is_finite_array = match arg.data_type() {
                    DataType::Float32 => {
                        let array = arg.as_any().downcast_ref::<Float32Array>().unwrap();
                        BooleanArray::from_unary(array, |a| a.is_finite())
                    }
                    DataType::Float64 => {
                        let array = arg.as_any().downcast_ref::<Float64Array>().unwrap();
                        BooleanArray::from_unary(array, |a| a.is_finite())
                    }
                    _ => {
                        // No other type can be non-finite
                        BooleanArray::from(vec![true; arg.len()])
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(is_finite_array)))
            }
        }
    }
}

lazy_static! {
    pub static ref ISFINITE_UDF: ScalarUDF = ScalarUDF::from(IsFiniteUDF::new());
}
