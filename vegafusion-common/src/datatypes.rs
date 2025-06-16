use crate::error::{Result, ResultWithContext};
use arrow::datatypes::DataType;
use datafusion_common::DFSchema;
use datafusion_expr::{lit, when, Expr, ExprSchemable, TryCast};
use datafusion_functions::datetime::expr_fn::to_timestamp_millis;

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
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
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
        DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

pub fn is_string_datatype(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
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
        when(value.clone().is_null(), lit(false)).otherwise(value)?
    } else if matches!(
        dtype,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    ) {
        let not_eq_zero = value.not_eq(lit(0));
        when(not_eq_zero.clone().is_null(), lit(false)).otherwise(not_eq_zero)?
    } else {
        // TODO: JavaScript falsey cast
        //  - empty string to false
        //  - NaN to false
        let cast_expr = Expr::TryCast(TryCast {
            expr: Box::new(value),
            data_type: DataType::Boolean,
        });
        when(cast_expr.clone().is_null(), lit(false)).otherwise(cast_expr)?
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
        Expr::TryCast(TryCast {
            expr: Box::new(to_timestamp_millis(vec![value])),
            data_type: DataType::Int64,
        })
    } else {
        // Cast non-numeric types (like UTF-8) to Float64
        Expr::TryCast(TryCast {
            expr: Box::new(value),
            data_type: DataType::Float64,
        })
    };

    Ok(numeric_value)
}

/// Cast an expression to Utf8 if not already Utf8. If already numeric, don't perform cast.
pub fn to_string(value: Expr, schema: &DFSchema) -> Result<Expr> {
    let dtype = data_type(&value, schema)?;
    let utf8_value =
        if dtype == DataType::Utf8 || dtype == DataType::LargeUtf8 || dtype == DataType::Utf8View {
            value
        } else {
            Expr::TryCast(TryCast {
                expr: Box::new(value),
                data_type: DataType::Utf8,
            })
        };

    Ok(utf8_value)
}

pub fn is_null_literal(value: &Expr) -> bool {
    if let Expr::Literal(literal, _) = &value {
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
        Ok(Expr::TryCast(TryCast {
            expr: Box::new(value),
            data_type: cast_dtype.clone(),
        }))
    }
}
