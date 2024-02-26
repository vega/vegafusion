use std::any::Any;
use vegafusion_common::datafusion_expr::{ScalarUDFImpl, TypeSignature};
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, Volatility},
};

#[derive(Debug, Clone)]
pub struct DateTruncTzUDF {
    signature: Signature,
}

impl Default for DateTruncTzUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl DateTruncTzUDF {
    pub fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Utf8, // part
                    DataType::Date32,
                    DataType::Utf8, // timezone
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8, // part
                    DataType::Date64,
                    DataType::Utf8, // timezone
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8, // part
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Utf8, // timezone
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8, // part
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Utf8, // timezone
                ]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for DateTruncTzUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_trunc_tz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> vegafusion_common::datafusion_common::Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn invoke(
        &self,
        _args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        unimplemented!("date_trunc_tz function is not implemented by DataFusion")
    }
}

lazy_static! {
    pub static ref DATE_TRUNC_TZ_UDF: ScalarUDF = ScalarUDF::from(DateTruncTzUDF::new());
}
