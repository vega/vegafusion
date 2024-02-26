use std::any::Any;
use vegafusion_common::datafusion_common::DataFusionError;
use vegafusion_common::datafusion_expr::{ScalarUDFImpl, TypeSignature};
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility},
};

#[derive(Debug, Clone)]
pub struct DateAddTzUDF {
    signature: Signature,
}

impl Default for DateAddTzUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl DateAddTzUDF {
    pub fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int32,
                    DataType::Date32,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int32,
                    DataType::Date64,
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int32,
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Int32,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for DateAddTzUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_add_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke(
        &self,
        _args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        unimplemented!("date_add_tz function is not implemented by DataFusion")
    }
}

fn make_date_add_tz_udf() -> ScalarUDF {
    ScalarUDF::from(DateAddTzUDF::new())
}

lazy_static! {
    pub static ref DATE_ADD_TZ_UDF: ScalarUDF = make_date_add_tz_udf();
}
