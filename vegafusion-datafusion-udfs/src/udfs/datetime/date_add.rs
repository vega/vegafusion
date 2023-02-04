use std::sync::Arc;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_date_add_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |_args: &[ColumnarValue]| {
        unimplemented!("date_add function is not implemented by DataFusion")
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    let signature = Signature::exact(
        vec![
            DataType::Utf8,
            DataType::Int32,
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ],
        Volatility::Immutable,
    );

    ScalarUDF::new("date_add", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref DATE_ADD_UDF: ScalarUDF = make_date_add_udf();
}
