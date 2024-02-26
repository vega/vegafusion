use crate::udfs::datetime::to_utc_timestamp::to_timestamp_ms;
use chrono::NaiveDateTime;
use std::any::Any;
use std::sync::Arc;
use vegafusion_common::datafusion_expr::ScalarUDFImpl;
use vegafusion_common::{
    arrow::{
        array::{ArrayRef, StringArray, TimestampMillisecondArray},
        datatypes::{DataType, TimeUnit},
    },
    datafusion_common::{DataFusionError, ScalarValue},
    datafusion_expr::{ColumnarValue, ScalarUDF, Signature, TypeSignature, Volatility},
};

#[derive(Debug, Clone)]
pub struct FormatTimestampUDF {
    signature: Signature,
}

impl Default for FormatTimestampUDF {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatTimestampUDF {
    pub fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Date64, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Utf8,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }
}

impl ScalarUDFImpl for FormatTimestampUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "format_timestamp"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> vegafusion_common::datafusion_common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke(
        &self,
        args: &[ColumnarValue],
    ) -> vegafusion_common::datafusion_common::Result<ColumnarValue> {
        // Argument order
        // [0] data array
        let data_array = match &args[0] {
            ColumnarValue::Array(array) => array.clone(),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // [1] time format string
        let d3_format_str = if let ColumnarValue::Scalar(format_str) = &args[1] {
            format_str.to_string()
        } else {
            return Err(DataFusionError::Internal(
                "Expected format string to be a scalar".to_string(),
            ));
        };

        // Convert D3 format specification into chrono format specification
        let format_str = convert_d3_format_string(&d3_format_str);

        if matches!(data_array.data_type(), DataType::Null) {
            return Ok(ColumnarValue::Array(data_array));
        }

        let data_array = to_timestamp_ms(&data_array)?;

        let utc_millis_array = data_array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();

        let formatted = Arc::new(StringArray::from_iter(utc_millis_array.iter().map(
            |utc_millis| {
                utc_millis.map(|utc_millis| {
                    // Load as UTC datetime
                    let utc_seconds = utc_millis / 1_000;
                    let utc_nanos = (utc_millis % 1_000 * 1_000_000) as u32;
                    let naive_datetime = NaiveDateTime::from_timestamp_opt(utc_seconds, utc_nanos)
                        .expect("invalid or out-of-range datetime");

                    // Format as string
                    let formatted = naive_datetime.format(&format_str);
                    formatted.to_string()
                })
            },
        ))) as ArrayRef;

        // maybe back to scalar
        if formatted.len() != 1 {
            Ok(ColumnarValue::Array(formatted))
        } else {
            ScalarValue::try_from_array(&formatted, 0).map(ColumnarValue::Scalar)
        }
    }
}

fn convert_d3_format_string(d3_format_str: &str) -> String {
    // %f is microseconds in D3 but nanoseconds in chrono, this is %6f in chrono
    let format_str = d3_format_str.replace("%f", "%6f");

    // %L is milliseconds in D3, this is %3f in chrono
    format_str.replace("%L", "%3f")
}

lazy_static! {
    pub static ref FORMAT_TIMESTAMP_UDF: ScalarUDF = ScalarUDF::from(FormatTimestampUDF::new());
}
