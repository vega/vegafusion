use crate::error::{Result, VegaFusionError};
use arrow::array::{Array, ArrayRef, ListArray};
use datafusion_common::DataFusionError;

use arrow::datatypes::DataType;
pub use datafusion_common::ScalarValue;

#[cfg(feature = "json")]
use {
    arrow::array::new_empty_array,
    datafusion_common::utils::array_into_list_array,
    serde_json::{Map, Value},
    std::ops::Deref,
    std::sync::Arc,
};

// Prefix for special values JSON encoded as strings
pub const DATETIME_PREFIX: &str = "__$datetime:";

pub trait ScalarValueHelpers {
    #[cfg(feature = "json")]
    fn from_json(value: &Value) -> Result<ScalarValue>;

    #[cfg(feature = "json")]
    fn to_json(&self) -> Result<Value>;

    fn to_i32(&self) -> Result<i32>;
    fn to_f64(&self) -> Result<f64>;
    fn to_f64x2(&self) -> Result<[f64; 2]>;
    fn to_scalar_string(&self) -> Result<String>;
    fn negate(&self) -> Self;
}

impl ScalarValueHelpers for ScalarValue {
    #[cfg(feature = "json")]
    fn from_json(value: &Value) -> Result<ScalarValue> {
        let scalar_value = match value {
            Value::Null => {
                // Use None float64 for null
                ScalarValue::try_from(&DataType::Float64).unwrap()
            }
            Value::Bool(v) => ScalarValue::from(*v),
            Value::Number(v) => ScalarValue::from(v.as_f64().unwrap()),
            Value::String(v) => {
                if v.starts_with(DATETIME_PREFIX) {
                    let ms: i64 = v.strip_prefix(DATETIME_PREFIX).unwrap().parse().unwrap();
                    ScalarValue::Float64(Some(ms as f64))
                } else {
                    ScalarValue::from(v.as_str())
                }
            }
            Value::Object(values) => {
                let mut values: Vec<_> = values
                    .iter()
                    .map(|(name, val)| Ok((name.as_str(), ScalarValue::from_json(val)?)))
                    .collect::<Result<Vec<(&str, ScalarValue)>>>()?;

                // Sort keys for stability
                values.sort_by_key(|el| el.0);

                if values.is_empty() {
                    ScalarValue::from(vec![(
                        "__dummy",
                        ScalarValue::try_from(&DataType::Float64).unwrap(),
                    )])
                } else {
                    ScalarValue::from(values)
                }
            }
            Value::Array(elements) => {
                let array: ListArray = if elements.is_empty() {
                    array_into_list_array(Arc::new(new_empty_array(&DataType::Float64)), true)
                } else {
                    let elements: Vec<_> = elements
                        .iter()
                        .map(ScalarValue::from_json)
                        .collect::<Result<Vec<ScalarValue>>>()?;

                    array_into_list_array(ScalarValue::iter_to_array(elements)?, true)
                };

                ScalarValue::List(Arc::new(array))
            }
        };
        Ok(scalar_value)
    }

    #[cfg(feature = "json")]
    fn to_json(&self) -> Result<Value> {
        let res = match self {
            ScalarValue::Boolean(Some(v)) => Value::from(*v),
            ScalarValue::Float32(Some(v)) => Value::from(*v),
            ScalarValue::Float64(Some(v)) => Value::from(*v),
            ScalarValue::Int8(Some(v)) => Value::from(*v),
            ScalarValue::Int16(Some(v)) => Value::from(*v),
            ScalarValue::Int32(Some(v)) => Value::from(*v),
            ScalarValue::Int64(Some(v)) => Value::from(*v),
            ScalarValue::UInt8(Some(v)) => Value::from(*v),
            ScalarValue::UInt16(Some(v)) => Value::from(*v),
            ScalarValue::UInt32(Some(v)) => Value::from(*v),
            ScalarValue::UInt64(Some(v)) => Value::from(*v),
            ScalarValue::Utf8(Some(v)) => Value::from(v.clone()),
            ScalarValue::LargeUtf8(Some(v)) => Value::from(v.clone()),
            ScalarValue::Binary(Some(_v)) => {
                unimplemented!()
            }
            ScalarValue::LargeBinary(Some(_v)) => {
                unimplemented!()
            }
            ScalarValue::Date32(Some(v)) => {
                let ms_per_day: i32 = 1000 * 60 * 60 * 24;
                let utc_millis = *v * ms_per_day;
                Value::from(utc_millis)
            }
            ScalarValue::Date64(Some(v)) => {
                // To UTC integer milliseconds (alread in UTC)
                Value::from(*v)
            }
            ScalarValue::TimestampSecond(Some(v), _) => {
                let millis = v * 1_000;
                Value::from(millis)
            }
            ScalarValue::TimestampMillisecond(Some(v), _) => {
                let millis = *v;
                Value::from(millis)
            }
            ScalarValue::TimestampMicrosecond(Some(v), _) => {
                let millis = *v / 1_000;
                Value::from(millis)
            }
            ScalarValue::TimestampNanosecond(Some(v), _) => {
                let millis = *v / 1_000_000;
                Value::from(millis)
            }
            ScalarValue::IntervalYearMonth(Some(_v)) => {
                unimplemented!()
            }
            ScalarValue::IntervalDayTime(Some(_v)) => {
                unimplemented!()
            }
            ScalarValue::List(a) => {
                let values = a
                    .value(0)
                    .to_scalar_vec()?
                    .into_iter()
                    .map(|v| v.to_json())
                    .collect::<Result<Vec<_>>>()?;

                Value::Array(values)
            }
            ScalarValue::Struct(sa) => {
                let mut pairs: Map<String, Value> = Default::default();
                for (col_ind, field) in sa.fields().deref().iter().enumerate() {
                    let column = sa.column(col_ind);
                    pairs.insert(
                        field.name().clone(),
                        ScalarValue::try_from_array(column, 0)?.to_json()?,
                    );
                }
                Value::Object(pairs)
            }
            _ => Value::Null,
        };
        Ok(res)
    }

    fn to_i32(&self) -> Result<i32> {
        Ok(match self {
            ScalarValue::Float32(Some(e)) => *e as i32,
            ScalarValue::Float64(Some(e)) => *e as i32,
            ScalarValue::Int8(Some(e)) => *e as i32,
            ScalarValue::Int16(Some(e)) => *e as i32,
            ScalarValue::Int32(Some(e)) => *e,
            ScalarValue::Int64(Some(e)) => *e as i32,
            ScalarValue::UInt8(Some(e)) => *e as i32,
            ScalarValue::UInt16(Some(e)) => *e as i32,
            ScalarValue::UInt32(Some(e)) => *e as i32,
            ScalarValue::UInt64(Some(e)) => *e as i32,
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "Cannot convert {self} to i32"
                )))
            }
        })
    }

    fn to_f64(&self) -> Result<f64> {
        Ok(match self {
            ScalarValue::Float32(Some(e)) => *e as f64,
            ScalarValue::Float64(Some(e)) => *e,
            ScalarValue::Int8(Some(e)) => *e as f64,
            ScalarValue::Int16(Some(e)) => *e as f64,
            ScalarValue::Int32(Some(e)) => *e as f64,
            ScalarValue::Int64(Some(e)) => *e as f64,
            ScalarValue::UInt8(Some(e)) => *e as f64,
            ScalarValue::UInt16(Some(e)) => *e as f64,
            ScalarValue::UInt32(Some(e)) => *e as f64,
            ScalarValue::UInt64(Some(e)) => *e as f64,
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "Cannot convert {self} to f64"
                )))
            }
        })
    }

    fn to_f64x2(&self) -> Result<[f64; 2]> {
        if let ScalarValue::List(array) = self {
            let elements = array.value(0).to_scalar_vec()?;
            if let [v0, v1] = elements.as_slice() {
                return Ok([v0.to_f64()?, v1.to_f64()?]);
            }
        }
        Err(VegaFusionError::internal(format!(
            "Cannot convert {self} to [f64; 2]"
        )))
    }

    fn to_scalar_string(&self) -> Result<String> {
        Ok(match self {
            ScalarValue::Utf8(Some(value)) => value.clone(),
            ScalarValue::LargeUtf8(Some(value)) => value.clone(),
            _ => {
                return Err(VegaFusionError::internal(format!(
                    "Cannot convert {self} to String"
                )))
            }
        })
    }

    fn negate(&self) -> Self {
        match self {
            ScalarValue::Float32(Some(e)) => ScalarValue::Float32(Some(-*e)),
            ScalarValue::Float64(Some(e)) => ScalarValue::Float64(Some(-*e)),
            ScalarValue::Int8(Some(e)) => ScalarValue::Int8(Some(-*e)),
            ScalarValue::Int16(Some(e)) => ScalarValue::Int16(Some(-*e)),
            ScalarValue::Int32(Some(e)) => ScalarValue::Int32(Some(-*e)),
            ScalarValue::Int64(Some(e)) => ScalarValue::Int64(Some(-*e)),
            ScalarValue::UInt8(Some(e)) => ScalarValue::Int16(Some(-(*e as i16))),
            ScalarValue::UInt16(Some(e)) => ScalarValue::Int32(Some(-(*e as i32))),
            ScalarValue::UInt32(Some(e)) => ScalarValue::Int64(Some(-(*e as i64))),
            ScalarValue::UInt64(Some(e)) => ScalarValue::Int64(Some(-(*e as i64))),
            _ => self.clone(),
        }
    }
}

pub trait ArrayRefHelpers {
    fn to_scalar_vec(&self) -> std::result::Result<Vec<ScalarValue>, DataFusionError>;

    fn list_el_to_scalar_vec(&self) -> std::result::Result<Vec<ScalarValue>, DataFusionError>;

    fn list_el_len(&self) -> std::result::Result<usize, DataFusionError>;

    fn list_el_dtype(&self) -> std::result::Result<DataType, DataFusionError>;
}

impl ArrayRefHelpers for ArrayRef {
    /// Convert ArrayRef into vector of ScalarValues
    fn to_scalar_vec(&self) -> std::result::Result<Vec<ScalarValue>, DataFusionError> {
        (0..self.len())
            .map(|i| ScalarValue::try_from_array(self, i))
            .collect::<std::result::Result<Vec<_>, DataFusionError>>()
    }

    /// Extract Vec<ScalarValue> for single element ListArray (as is stored inside ScalarValue::List(arr))
    fn list_el_to_scalar_vec(&self) -> std::result::Result<Vec<ScalarValue>, DataFusionError> {
        let a = self
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(DataFusionError::Internal(
                "list_el_to_scalar_vec called on non-List type".to_string(),
            ))?;
        a.value(0).to_scalar_vec()
    }

    /// Extract length of single element ListArray
    fn list_el_len(&self) -> std::result::Result<usize, DataFusionError> {
        let a = self
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(DataFusionError::Internal(
                "list_el_len called on non-List type".to_string(),
            ))?;
        Ok(a.value(0).len())
    }

    /// Extract data type of single element ListArray
    fn list_el_dtype(&self) -> std::result::Result<DataType, DataFusionError> {
        let a = self
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or(DataFusionError::Internal(
                "list_el_len called on non-List type".to_string(),
            ))?;
        Ok(a.value(0).data_type().clone())
    }
}
