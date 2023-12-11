use std::sync::Arc;
use vegafusion_common::datafusion_expr::TypeSignature;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_date_add_tz_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        unimplemented!("date_add_tz function is not implemented by DataFusion")
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

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

    ScalarUDF::new("date_add_tz", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref DATE_ADD_TZ_UDF: ScalarUDF = make_date_add_tz_udf();
}
