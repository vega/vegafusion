pub mod conditional;
pub mod config;
pub mod identifier;
pub mod literal;
pub mod logical;
pub mod unary;
pub mod utils;

use crate::error::Result;
use crate::expression::ast::base::Expression;
use crate::expression::compiler::conditional::compile_conditional;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::identifier::compile_identifier;
use crate::expression::compiler::literal::compile_literal;
use crate::expression::compiler::logical::compile_logical;
use crate::expression::compiler::unary::compile_unary;
use datafusion::logical_plan::{DFSchema, Expr};
use utils::UNIT_SCHEMA;

/// Function to compile a parsed expression into a CompiledExpression, given a scope containing
/// a SignalValue for every unbound identifier in the expression.
pub fn compile(
    expr: &Expression,
    config: &CompilationConfig,
    schema: Option<&DFSchema>,
) -> Result<Expr> {
    let schema = schema.unwrap_or_else(|| &(*UNIT_SCHEMA));

    match expr {
        Expression::Literal(node) => Ok(compile_literal(node)),
        Expression::Identifier(node) => compile_identifier(node, config),
        Expression::UnaryExpression(node) => compile_unary(node, config, schema),
        Expression::ConditionalExpression(node) => compile_conditional(node, config, schema),
        Expression::LogicalExpression(node) => compile_logical(node, config, schema),
        // Expression::BinaryExpression(node) => compile_binary(node, config, schema),
        // Expression::ArrayExpression(node) => compile_array(node, config, schema),
        // Expression::ObjectExpression(node) => compile_object(node, config, schema),
        // Expression::CallExpression(node) => compile_call(node, config, schema),
        // Expression::MemberExpression(node) => compile_member(node, config, schema),
        _ => todo!(),
    }
}

#[cfg(test)]
mod test_compile {

    use crate::expression::compiler::compile;
    use crate::expression::compiler::config::CompilationConfig;
    use crate::expression::compiler::utils::ExprHelpers;
    use crate::expression::parser::parse;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_plan::{Expr, Operator};
    use datafusion::prelude::lit;
    use datafusion::scalar::ScalarValue;
    use std::collections::HashMap;

    #[test]
    fn test_compile_literal_float() {
        let expression = parse(r#"22.50"#).unwrap();
        let result = compile(&expression, &Default::default(), None).unwrap();
        assert_eq!(result, lit(22.5))
    }

    #[test]
    fn test_compile_literal_integer() {
        let expression = parse(r#"22"#).unwrap();
        let result = compile(&expression, &Default::default(), None).unwrap();
        assert_eq!(result, lit(22.0))
    }

    #[test]
    fn test_compile_literal_string() {
        let expression = parse(r#"'Hello, world!'"#).unwrap();
        let result = compile(&expression, &Default::default(), None).unwrap();
        assert_eq!(result, lit("Hello, world!"))
    }

    #[test]
    fn test_compile_literal_boolean() {
        let expression = parse(r#" false "#).unwrap();
        let result = compile(&expression, &Default::default(), None).unwrap();
        assert_eq!(result, lit(false))
    }

    #[test]
    fn test_compile_identifier_in_scope() {
        let scope: HashMap<String, ScalarValue> =
            vec![("foo".to_string(), ScalarValue::Int32(Some(42)))]
                .into_iter()
                .collect();
        let config = CompilationConfig {
            signal_scope: scope,
            ..Default::default()
        };

        let expr = parse("foo").unwrap();
        let result_expr = compile(&expr, &config, None).unwrap();
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, lit(42));

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(42);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_unary_neg() {
        let expr = parse("-(23.5)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let expected_expr = Expr::Negative(Box::new(lit(23.5)));
        assert_eq!(result_expr, expected_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(-23.5);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_unary_pos() {
        let expr = parse("+'72'").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        // plus prefix on a string should result in a numeric cast
        let expected_expr = Expr::Cast {
            expr: Box::new(lit("72")),
            data_type: DataType::Float64,
        };
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(72.0);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_unary_not() {
        let expr = parse("!32").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        // unary not should cast numeric value to boolean
        let expected_expr = Expr::Not(Box::new(Expr::Cast {
            expr: Box::new(lit(32.0)),
            data_type: DataType::Boolean,
        }));
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(false);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_conditional() {
        let expr = parse("32? 7: 9").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let expected_expr = Expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(Expr::Cast {
                    expr: Box::new(lit(32.0)),
                    data_type: DataType::Boolean,
                }),
                Box::new(lit(7.0)),
            )],
            else_expr: Some(Box::new(lit(9.0))),
        };
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(7.0);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_logical_boolean() {
        let expr = parse("false || true").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(lit(false)),
            op: Operator::Or,
            right: Box::new(lit(true)),
        };
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(true);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_eval_logical_non_boolean() {
        let expr = parse("5 && 55").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let expected_expr = Expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(Expr::Cast {
                    expr: Box::new(lit(5.0)),
                    data_type: DataType::Boolean,
                }),
                Box::new(lit(55.0)),
            )],
            else_expr: Some(Box::new(lit(5.0))),
        };
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(55.0);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }
}
