use std::sync::Arc;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_utc_timestamp_to_str_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        unimplemented!("utc_timestamp_to_str function is not implemented by DataFusion")
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    let signature = Signature::exact(
        vec![
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Utf8,
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("utc_timestamp_to_str", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref UTC_TIMESTAMP_TO_STR_UDF: ScalarUDF = make_utc_timestamp_to_str_udf();
}
