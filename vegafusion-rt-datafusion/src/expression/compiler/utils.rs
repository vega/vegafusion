/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use datafusion::arrow::array::{ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::{and, Column, DFSchema, Expr, ExprSchemable};
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr, PhysicalPlanner};
use datafusion::scalar::ScalarValue;

use std::collections::HashSet;
use std::convert::TryFrom;

use datafusion::execution::context::{default_session_builder, SessionState};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::BuiltinScalarFunction;
use std::sync::Arc;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

lazy_static! {
    pub static ref UNIT_RECORD_BATCH: RecordBatch = RecordBatch::try_from_iter(vec![(
        "__unit__",
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef
    )])
    .unwrap();
    pub static ref UNIT_SCHEMA: DFSchema =
        DFSchema::try_from(UNIT_RECORD_BATCH.schema().as_ref().clone()).unwrap();
    pub static ref SESSION_STATE: SessionState = default_session_builder(Default::default());
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

pub fn is_integer_datatype(dtype: &DataType) -> bool {
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
    )
}

pub fn is_float_datatype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Float16 | DataType::Float32 | DataType::Float64
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
        and(Expr::IsNotNull(Box::new(value.clone())), value)
    } else {
        // TODO: JavaScript falsey cast
        //  - empty string to false
        //  - NaN to false
        and(
            Expr::Cast {
                expr: Box::new(value.clone()),
                data_type: DataType::Boolean,
            },
            Expr::IsNotNull(Box::new(value)),
        )
    };

    Ok(boolean_value)
}

/// Cast an expression to Float64 if not already numeric. If already numeric, don't perform cast.
pub fn to_numeric(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let numeric_value = if is_numeric_datatype(&dtype) {
        value
    } else if matches!(dtype, DataType::Timestamp(_, _)) {
        // Convert to milliseconds
        Expr::ScalarFunction {
            fun: BuiltinScalarFunction::ToTimestampMillis,
            args: vec![value],
        }
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

pub fn is_null_literal(value: &Expr) -> bool {
    if let Expr::Literal(literal) = &value {
        literal.is_null()
    } else {
        false
    }
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
            .create_physical_expr(self, schema, &physical_schema, &SESSION_STATE)
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
