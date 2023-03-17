use crate::udfs::datetime::from_utc_timestamp::from_utc_timestamp;
use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;
use datafusion_physical_expr::datetime_expressions;
use std::str::FromStr;
use std::sync::Arc;
use vegafusion_common::datafusion_expr::TypeSignature;
use vegafusion_common::{
    arrow::datatypes::{DataType, TimeUnit},
    datafusion_common::DataFusionError,
    datafusion_expr::{
        ColumnarValue, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature,
        Volatility,
    },
};

fn make_date_part_tz_udf() -> ScalarUDF {
    let scalar_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| {
        // [1] data array
        let timestamp_array = match &args[1] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        let timestamp_array = to_timestamp_ms(&timestamp_array)?;

        // [2] timezone string
        let tz_str = if let ColumnarValue::Scalar(default_input_tz) = &args[2] {
            default_input_tz.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected default_input_tz to be a scalar".to_string(),
            ));
        };

        // Convert timestamp to desired time zone
        let timestamp_in_tz = if tz_str == "UTC" {
            timestamp_array
        } else {
            let tz = chrono_tz::Tz::from_str(&tz_str).map_err(|_err| {
                DataFusionError::Internal(format!("Failed to parse {tz_str} as a timezone"))
            })?;
            from_utc_timestamp(timestamp_array, tz)?
        };
        let timestamp_in_tz = ColumnarValue::Array(timestamp_in_tz);

        // Use DataFusion's built-in date_part implementation
        datetime_expressions::date_part(&[
            args[0].clone(), // Part
            timestamp_in_tz, // Timestamp converted to timezone
        ])
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));

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

    ScalarUDF::new("date_part_tz", &signature, &return_type, &scalar_fn)
}

lazy_static! {
    pub static ref DATE_PART_TZ_UDF: ScalarUDF = make_date_part_tz_udf();
}
