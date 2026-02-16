use crate::expression::compiler::{compile, config::CompilationConfig};
use datafusion_expr::expr::{BinaryExpr, Case};
use datafusion_expr::{Expr, Operator};
use std::convert::TryFrom;
use std::sync::Arc;
use vegafusion_common::arrow::datatypes::DataType;
use vegafusion_common::data::scalar::is_empty_object_sentinel_fields;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_common::datafusion_common::ScalarValue;
use vegafusion_common::datatypes::{
    cast_to, data_type, is_null_literal, is_numeric_datatype, to_boolean,
};
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::{LogicalExpression, LogicalOperator};

pub fn compile_logical(
    node: &LogicalExpression,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    // Compile branches
    let mut compiled_lhs = compile(node.left(), config, Some(schema))?;
    let mut compiled_rhs = compile(node.right(), config, Some(schema))?;
    let lhs_for_truthiness = compiled_lhs.clone();

    let mut lhs_dtype = data_type(&compiled_lhs, schema)?;
    let mut rhs_dtype = data_type(&compiled_rhs, schema)?;

    normalize_struct_branches(
        &mut compiled_lhs,
        &mut lhs_dtype,
        &mut compiled_rhs,
        &mut rhs_dtype,
    )?;

    let new_expr = match (&lhs_dtype, &rhs_dtype) {
        (DataType::Boolean, DataType::Boolean) => {
            // If both are boolean, the use regular logical operation
            match node.to_operator() {
                LogicalOperator::Or => Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(compiled_lhs),
                    op: Operator::Or,
                    right: Box::new(compiled_rhs),
                }),
                LogicalOperator::And => Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(compiled_lhs),
                    op: Operator::And,
                    right: Box::new(compiled_rhs),
                }),
            }
        }
        _ => {
            // Not both boolean, compile to CASE expression so the results will be drawn
            // from the LHS and RHS
            let lhs_boolean = to_boolean(lhs_for_truthiness, schema)?;

            // If one side boolean and the other numeric, cast the boolean column to match the
            // numeric one since DataFusion doesn't allow this automatically (for good reason!)
            if is_numeric_datatype(&lhs_dtype) && rhs_dtype == DataType::Boolean {
                compiled_rhs = cast_to(compiled_rhs, &lhs_dtype, schema)?;
            } else if is_numeric_datatype(&rhs_dtype) && lhs_dtype == DataType::Boolean {
                compiled_lhs = cast_to(compiled_lhs, &rhs_dtype, schema)?;
            }

            if node.to_operator() == LogicalOperator::And
                && is_empty_object_struct_dtype(&lhs_dtype)
                && !matches!(rhs_dtype, DataType::Struct(_))
            {
                compiled_lhs = null_expr_for_type(&rhs_dtype)?;
            }

            match node.to_operator() {
                LogicalOperator::Or => Expr::Case(Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(lhs_boolean), Box::new(compiled_lhs))],
                    else_expr: Some(Box::new(compiled_rhs)),
                }),
                LogicalOperator::And => Expr::Case(Case {
                    expr: None,
                    when_then_expr: vec![(Box::new(lhs_boolean), Box::new(compiled_rhs))],
                    else_expr: Some(Box::new(compiled_lhs)),
                }),
            }
        }
    };

    Ok(new_expr)
}

fn normalize_struct_branches(
    compiled_lhs: &mut Expr,
    lhs_dtype: &mut DataType,
    compiled_rhs: &mut Expr,
    rhs_dtype: &mut DataType,
) -> Result<()> {
    match (&*lhs_dtype, &*rhs_dtype) {
        (DataType::Struct(lhs_fields), DataType::Struct(rhs_fields)) => {
            let lhs_is_empty = is_empty_object_sentinel_fields(lhs_fields);
            let rhs_is_empty = is_empty_object_sentinel_fields(rhs_fields);

            if lhs_is_empty && !rhs_is_empty {
                let target = DataType::Struct(rhs_fields.clone());
                *compiled_lhs = null_expr_for_type(&target)?;
                *lhs_dtype = target;
            } else if rhs_is_empty && !lhs_is_empty {
                let target = DataType::Struct(lhs_fields.clone());
                *compiled_rhs = null_expr_for_type(&target)?;
                *rhs_dtype = target;
            }
        }
        (DataType::Struct(lhs_fields), _) if is_null_literal(compiled_rhs) => {
            let target = DataType::Struct(lhs_fields.clone());
            *compiled_rhs = null_expr_for_type(&target)?;
            *rhs_dtype = target;
        }
        (_, DataType::Struct(rhs_fields)) if is_null_literal(compiled_lhs) => {
            let target = DataType::Struct(rhs_fields.clone());
            *compiled_lhs = null_expr_for_type(&target)?;
            *lhs_dtype = target;
        }
        _ => {}
    }
    Ok(())
}

fn null_expr_for_type(dtype: &DataType) -> Result<Expr> {
    let null_scalar = if let DataType::Struct(fields) = dtype {
        ScalarValue::Struct(Arc::new(
            vegafusion_common::arrow::array::StructArray::new_null(fields.clone(), 1),
        ))
    } else {
        ScalarValue::try_from(dtype)?
    };
    Ok(datafusion_expr::lit(null_scalar))
}

fn is_empty_object_struct_dtype(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Struct(fields) if is_empty_object_sentinel_fields(fields))
}
