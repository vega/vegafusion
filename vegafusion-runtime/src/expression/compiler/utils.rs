use datafusion_common::ScalarValue;
use datafusion_expr::expr::Cast;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{coalesce, lit, BuiltinScalarFunction, Expr, ExprSchemable};
use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};
use datafusion_physical_expr::execution_props::ExecutionProps;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;
use vegafusion_common::arrow::array::{ArrayRef, BooleanArray};
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::arrow::record_batch::RecordBatch;
use vegafusion_common::datafusion_common::{Column, DFSchema, DataFusionError};
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};

lazy_static! {
    pub static ref UNIT_RECORD_BATCH: RecordBatch = RecordBatch::try_from_iter(vec![(
        "__unit__",
        Arc::new(BooleanArray::from(vec![true])) as ArrayRef
    )])
    .unwrap();
    pub static ref UNIT_SCHEMA: DFSchema =
        DFSchema::try_from(UNIT_RECORD_BATCH.schema().as_ref().clone()).unwrap();
    // pub static ref SESSION_STATE: SessionState = default_session_builder(Default::default());
    // pub static ref PLANNER: DefaultPhysicalPlanner = Default::default();
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
    value.get_type(schema).with_context(|| {
        format!("Failed to infer datatype of expression: {value:?}\nschema: {schema:?}")
    })
}

/// Cast an expression to boolean if not already boolean
pub fn to_boolean(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let boolean_value = if matches!(dtype, DataType::Boolean) {
        coalesce(vec![value, lit(false)])
    } else if matches!(
        dtype,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        coalesce(vec![value.not_eq(lit(0)), lit(false)])
    } else {
        // TODO: JavaScript falsey cast
        //  - empty string to false
        //  - NaN to false
        coalesce(vec![
            Expr::Cast(Cast {
                expr: Box::new(value),
                data_type: DataType::Boolean,
            }),
            lit(false),
        ])
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
        Expr::Cast(Cast {
            expr: Box::new(value),
            data_type: DataType::Float64,
        })
    };

    Ok(numeric_value)
}

/// Cast an expression to Utf8 if not already Utf8. If already numeric, don't perform cast.
pub fn to_string(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let utf8_value = if dtype == DataType::Utf8 || dtype == DataType::LargeUtf8 {
        value
    } else {
        Expr::Cast(Cast {
            expr: Box::new(value),
            data_type: DataType::Utf8,
        })
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
        Ok(Expr::Cast(Cast {
            expr: Box::new(value),
            data_type: cast_dtype.clone(),
        }))
    }
}

pub trait ExprHelpers {
    fn columns(&self) -> Result<HashSet<Column>>;
    fn eval_to_scalar(&self) -> Result<ScalarValue>;
}

impl ExprHelpers for Expr {
    fn columns(&self) -> Result<HashSet<Column>> {
        let mut columns: HashSet<Column> = HashSet::new();
        expr_to_columns(self, &mut columns)
            .with_context(|| format!("Failed to collect columns from expression: {self:?}"))?;
        Ok(columns)
    }

    fn eval_to_scalar(&self) -> Result<ScalarValue> {
        let simplifier = ExprSimplifier::new(VfSimplifyInfo::from(UNIT_SCHEMA.deref().clone()));
        let simplified_expr = simplifier.simplify(self.clone())?;
        if let Expr::Literal(scalar) = simplified_expr {
            Ok(scalar)
        } else {
            Err(VegaFusionError::internal(format!(
                "Failed to evaluate expression to scalar value: {self}"
            )))
        }
    }
}

/// In order to simplify expressions, DataFusion must have information
/// about the expressions.
///
/// You can provide that information using DataFusion [DFSchema]
/// objects or from some other implemention
pub struct VfSimplifyInfo {
    /// The input schema
    schema: DFSchema,

    /// Execution specific details needed for constant evaluation such
    /// as the current time for `now()` and [VariableProviders]
    execution_props: ExecutionProps,
}

impl SimplifyInfo for VfSimplifyInfo {
    fn is_boolean_type(&self, expr: &Expr) -> std::result::Result<bool, DataFusionError> {
        Ok(matches!(expr.get_type(&self.schema)?, DataType::Boolean))
    }

    fn nullable(&self, expr: &Expr) -> std::result::Result<bool, DataFusionError> {
        expr.nullable(&self.schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }
}

impl From<DFSchema> for VfSimplifyInfo {
    fn from(schema: DFSchema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }
}
