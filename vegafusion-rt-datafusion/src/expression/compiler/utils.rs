use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::ExecutionContextState;
use datafusion::logical_plan::{Column, DFSchema, Expr};
use datafusion::optimizer::utils::expr_to_columns;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

// Prefix for special values JSON encoded as strings
pub const DATETIME_PREFIX: &str = "__$datetime:";

lazy_static! {
    pub static ref UNIT_RECORD_BATCH: RecordBatch = RecordBatch::try_from_iter(vec![(
        "__unit__",
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef
    )])
    .unwrap();
    pub static ref UNIT_SCHEMA: DFSchema =
        DFSchema::try_from(UNIT_RECORD_BATCH.schema().as_ref().clone()).unwrap();
    pub static ref CTX_STATE: ExecutionContextState = ExecutionContextState::new();
    pub static ref PLANNER: DefaultPhysicalPlanner = Default::default();
}

pub fn is_numeric_datatype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
    )
}

pub fn is_string_datatype(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Utf8 | DataType::LargeUtf8)
}

/// get datatype for expression
pub fn data_type(value: &Expr, schema: &DFSchema) -> Result<DataType> {
    value
        .get_type(schema)
        .with_context(|| format!("Failed to infer datatype of expression: {:?}", value))
}

/// Cast an expression to boolean if not already boolean
pub fn to_boolean(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let boolean_value = if matches!(dtype, DataType::Boolean) {
        value
    } else {
        // TODO: JavaScript falsey cast
        //  - empty string to false
        //  - NaN to false
        //  - NULL to false
        Expr::Cast {
            expr: Box::new(value),
            data_type: DataType::Boolean,
        }
    };

    Ok(boolean_value)
}

/// Cast an expression to Float64 if not already numeric. If already numeric, don't perform cast.
pub fn to_numeric(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let numeric_value = if is_numeric_datatype(&dtype) {
        value
    } else {
        // Cast non-numeric types (like UTF-8) to Float64
        Expr::Cast {
            expr: Box::new(value),
            data_type: DataType::Float64,
        }
    };

    Ok(numeric_value)
}

/// Cast an expression to Utf8 if not already Utf8. If already numeric, don't perform cast.
pub fn to_string(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let utf8_value = if dtype == DataType::Utf8 || dtype == DataType::LargeUtf8 {
        value
    } else {
        Expr::Cast {
            expr: Box::new(value),
            data_type: DataType::Utf8,
        }
    };

    Ok(utf8_value)
}

pub fn cast_to(value: Expr, cast_dtype: &DataType, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    if &dtype == cast_dtype {
        Ok(value)
    } else {
        // Cast non-numeric types (like UTF-8) to Float64
        Ok(Expr::Cast {
            expr: Box::new(value),
            data_type: cast_dtype.clone(),
        })
    }
}

pub trait ExprHelpers {
    fn columns(&self) -> Result<HashSet<Column>>;
    fn to_physical_expr(&self, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>>;
    fn eval_to_scalar(&self) -> Result<ScalarValue>;
    fn eval_to_column(&self, record_batch: &RecordBatch) -> Result<ColumnarValue>;
}

impl ExprHelpers for Expr {
    fn columns(&self) -> Result<HashSet<Column>> {
        let mut columns: HashSet<Column> = HashSet::new();
        expr_to_columns(self, &mut columns)
            .with_context(|| format!("Failed to collect columns from expression: {:?}", self))?;
        Ok(columns)
    }

    fn to_physical_expr(&self, schema: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
        let physical_schema =
            Schema::new(schema.fields().iter().map(|f| f.field().clone()).collect());

        PLANNER
            .create_physical_expr(self, schema, &physical_schema, &CTX_STATE)
            .with_context(|| format!("Failed to create PhysicalExpr from {:?}", self))
    }

    fn eval_to_scalar(&self) -> Result<ScalarValue> {
        if !self.columns()?.is_empty() {
            return Err(VegaFusionError::compilation(&format!(
                "Cannot eval_to_scalar for Expr with column references: {:?}",
                self
            )));
        }

        let physical_expr = self.to_physical_expr(&UNIT_SCHEMA)?;

        let col_result = physical_expr
            .evaluate(&UNIT_RECORD_BATCH)
            .with_context(|| format!("Failed to evaluate expression: {:?}", self))?;

        match col_result {
            ColumnarValue::Scalar(scalar) => Ok(scalar),
            ColumnarValue::Array(array) => {
                if array.len() != 1 {
                    return Err(VegaFusionError::compilation(&format!(
                        "Unexpected non-scalar array result when evaluate expr: {:?}",
                        self
                    )));
                }
                ScalarValue::try_from_array(&array, 0).with_context(|| {
                    format!(
                        "Failed to convert scalar array result to ScalarValue in expr: {:?}",
                        self
                    )
                })
            }
        }
    }

    fn eval_to_column(&self, record_batch: &RecordBatch) -> Result<ColumnarValue> {
        let schema = DFSchema::try_from(record_batch.schema().as_ref().clone()).unwrap();

        let physical_expr = self.to_physical_expr(&schema)?;

        let col_result = physical_expr
            .evaluate(record_batch)
            .with_context(|| format!("Failed to evaluate expression: {:?}", self))?;

        let col_result = if let ColumnarValue::Scalar(scalar) = col_result {
            // Convert scalar to array with length matching record batch
            ColumnarValue::Array(scalar.to_array_of_size(record_batch.num_rows()))
        } else {
            col_result
        };

        Ok(col_result)
    }
}

pub trait ScalarValueHelpers {
    // fn from_json(value: &Value) -> Result<ScalarValue>;
    // fn to_json(&self) -> Result<Value>;

    fn to_f64(&self) -> Result<f64>;
    fn to_f64x2(&self) -> Result<[f64; 2]>;
}

impl ScalarValueHelpers for ScalarValue {
    // fn from_json(value: &Value) -> Result<ScalarValue> {
    //     let scalar_value = match value {
    //         Value::Null => {
    //             // Use None float64 for null
    //             ScalarValue::try_from(&DataType::Float64).unwrap()
    //         }
    //         Value::Bool(v) => ScalarValue::from(*v),
    //         Value::Number(v) => ScalarValue::from(v.as_f64().unwrap()),
    //         Value::String(v) => {
    //             if v.starts_with(DATETIME_PREFIX) {
    //                 let ms: i64 = v.strip_prefix(DATETIME_PREFIX).unwrap().parse().unwrap();
    //                 ScalarValue::TimestampMillisecond(Some(ms))
    //             } else {
    //                 ScalarValue::from(v.as_str())
    //             }
    //         }
    //         Value::Object(values) => {
    //             let mut values: Vec<_> = values
    //                 .iter()
    //                 .map(|(name, val)| Ok((name.as_str(), ScalarValue::from_json(val)?)))
    //                 .collect::<Result<Vec<(&str, ScalarValue)>>>()?;
    //
    //             // Sort keys for stability
    //             values.sort_by_key(|el| el.0);
    //
    //             ScalarValue::from(values)
    //         }
    //         Value::Array(elements) => {
    //             let (elements, dtype) = if elements.is_empty() {
    //                 (Vec::new(), DataType::Float64)
    //             } else {
    //                 let elements: Vec<_> = elements
    //                     .iter()
    //                     .map(|e| ScalarValue::from_json(e))
    //                     .collect::<Result<Vec<ScalarValue>>>()?;
    //                 let dtype = elements[0].get_datatype();
    //                 (elements, dtype)
    //             };
    //
    //             ScalarValue::List(Some(Box::new(elements)), Box::new(dtype))
    //         }
    //     };
    //     Ok(scalar_value)
    // }
    //
    // fn to_json(&self) -> Result<Value> {
    //     let res = match self {
    //         ScalarValue::Boolean(Some(v)) => Value::from(*v),
    //         ScalarValue::Float32(Some(v)) => Value::from(*v),
    //         ScalarValue::Float64(Some(v)) => Value::from(*v),
    //         ScalarValue::Int8(Some(v)) => Value::from(*v),
    //         ScalarValue::Int16(Some(v)) => Value::from(*v),
    //         ScalarValue::Int32(Some(v)) => Value::from(*v),
    //         ScalarValue::Int64(Some(v)) => Value::from(*v),
    //         ScalarValue::UInt8(Some(v)) => Value::from(*v),
    //         ScalarValue::UInt16(Some(v)) => Value::from(*v),
    //         ScalarValue::UInt32(Some(v)) => Value::from(*v),
    //         ScalarValue::UInt64(Some(v)) => Value::from(*v),
    //         ScalarValue::Utf8(Some(v)) => Value::from(v.clone()),
    //         ScalarValue::LargeUtf8(Some(v)) => Value::from(v.clone()),
    //         ScalarValue::Binary(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::LargeBinary(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::Date32(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::Date64(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::TimestampSecond(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::TimestampMillisecond(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::TimestampMicrosecond(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::TimestampNanosecond(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::IntervalYearMonth(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::IntervalDayTime(Some(_v)) => {
    //             unimplemented!()
    //         }
    //         ScalarValue::List(Some(v), _) => Value::Array(
    //             v.clone()
    //                 .into_iter()
    //                 .map(|v| v.to_json())
    //                 .collect::<Result<Vec<_>>>()?,
    //         ),
    //         ScalarValue::Struct(Some(v), fields) => {
    //             let mut pairs: Map<String, Value> = Default::default();
    //             for (val, field) in v.iter().zip(fields.deref()) {
    //                 pairs.insert(field.name().clone(), val.to_json()?);
    //             }
    //             Value::Object(pairs)
    //         }
    //         _ => Value::Null,
    //     };
    //
    //     Ok(res)
    // }

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
                return Err(VegaFusionError::internal(&format!("Cannot convert {} to f64", self)))
            },
        })
    }

    fn to_f64x2(&self) -> Result<[f64; 2]> {
        if let ScalarValue::List(Some(elements), _) = self {
            if let [v0, v1] = elements.as_slice() {
                return Ok([v0.to_f64()?, v1.to_f64()?]);
            }
        }
        return Err(VegaFusionError::internal(&format!("Cannot convert {} to [f64; 2]", self)))
    }
}
