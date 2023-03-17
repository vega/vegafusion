use std::sync::Arc;
use vegafusion_common::datafusion_expr::TypeSignature;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_date_trunc_tz_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        unimplemented!("date_trunc_tz function is not implemented by DataFusion")
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

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

    ScalarUDF::new("date_trunc_tz", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref DATE_TRUNC_TZ_UDF: ScalarUDF = make_date_trunc_tz_udf();
}
