/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
pub mod array;
pub mod binary;
pub mod builtin_functions;
pub mod call;
pub mod conditional;
pub mod config;
pub mod identifier;
pub mod literal;
pub mod logical;
pub mod member;
pub mod object;
pub mod unary;
pub mod utils;

use crate::expression::compiler::array::compile_array;
use crate::expression::compiler::binary::compile_binary;
use crate::expression::compiler::call::compile_call;
use crate::expression::compiler::conditional::compile_conditional;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::identifier::compile_identifier;
use crate::expression::compiler::literal::compile_literal;
use crate::expression::compiler::logical::compile_logical;
use crate::expression::compiler::member::compile_member;
use crate::expression::compiler::object::compile_object;
use crate::expression::compiler::unary::compile_unary;
use datafusion::logical_plan::{DFSchema, Expr};
use utils::UNIT_SCHEMA;

use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::{expression::Expr as vfExpr, Expression};

/// Function to compile a parsed expression into a CompiledExpression, given a scope containing
/// a SignalValue for every unbound identifier in the expression.
pub fn compile(
    expr: &Expression,
    config: &CompilationConfig,
    schema: Option<&DFSchema>,
) -> Result<Expr> {
    let schema = schema.unwrap_or(&(*UNIT_SCHEMA));
    let expr = expr.expr.as_ref().unwrap();
    use vfExpr::*;
    match expr {
        Literal(node) => Ok(compile_literal(node)),
        Identifier(node) => compile_identifier(node, config),
        Unary(node) => compile_unary(node, config, schema),
        Conditional(node) => compile_conditional(node, config, schema),
        Logical(node) => compile_logical(node, config, schema),
        Binary(node) => compile_binary(node, config, schema),
        Array(node) => compile_array(node, config, schema),
        Object(node) => compile_object(node, config, schema),
        Member(node) => compile_member(node, config, schema),
        Call(node) => compile_call(node, config, schema),
    }
}

#[cfg(test)]
mod test_compile {

    use crate::expression::compiler::array::array_constructor_udf;
    use crate::expression::compiler::compile;
    use crate::expression::compiler::config::CompilationConfig;
    use crate::expression::compiler::object::make_object_constructor_udf;
    use crate::expression::compiler::utils::ExprHelpers;
    use vegafusion_core::expression::parser::parse;

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::{
        array::{ArrayRef, Float64Array, StructArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::logical_plan::{and, DFSchema, Expr, Operator};

    use crate::task_graph::timezone::RuntimeTzConfig;
    use datafusion::physical_plan::ColumnarValue;
    use datafusion::prelude::{col, concat, lit};
    use datafusion::scalar::ScalarValue;
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::sync::Arc;

    // use vegafusion_client::expression::parser::parse;

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
        let expected_expr = and(
            Expr::Cast {
                expr: Box::new(lit(32.0)),
                data_type: DataType::Boolean,
            },
            Expr::is_not_null(lit(32.0)),
        )
        .not();
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
                Box::new(and(
                    Expr::Cast {
                        expr: Box::new(lit(32.0)),
                        data_type: DataType::Boolean,
                    },
                    Expr::is_not_null(lit(32.0)),
                )),
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
    fn test_compile_logical_non_boolean() {
        let expr = parse("5 && 55").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let expected_expr = Expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(and(
                    Expr::Cast {
                        expr: Box::new(lit(5.0)),
                        data_type: DataType::Boolean,
                    },
                    Expr::is_not_null(lit(5.0)),
                )),
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

    #[test]
    fn test_compile_binary_mixed() {
        let expr = parse("1 + +'2' + true * 10").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        // 1 + +'2'
        let t1 = Expr::BinaryExpr {
            left: Box::new(lit(1.0)),
            op: Operator::Plus,
            right: Box::new(Expr::Cast {
                expr: Box::new(lit("2")),
                data_type: DataType::Float64,
            }),
        };

        // true * 10
        let t2 = Expr::BinaryExpr {
            left: Box::new(Expr::Cast {
                expr: Box::new(lit(true)),
                data_type: DataType::Float64,
            }),
            op: Operator::Multiply,
            right: Box::new(lit(10.0)),
        };

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(t1),
            op: Operator::Plus,
            right: Box::new(t2),
        };

        println!("{:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::Float64(Some(13.0));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_binary_string_addition() {
        let expr = parse("'2' + '4'").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = concat(&[lit("2"), lit("4")]);
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::Utf8(Some("24".to_string()));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_binary_loose_equality() {
        let expr = parse("'2.0' == 2").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(Expr::Cast {
                expr: Box::new(lit("2.0")),
                data_type: DataType::Float64,
            }),
            op: Operator::Eq,
            right: Box::new(lit(2.0)),
        };

        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::Boolean(Some(true));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_binary_strict_equality() {
        let expr = parse("'2.0' === 2").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        // Types don't match, so this is compiled to the literal `false`
        let expected_expr = lit(false);
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::Boolean(Some(false));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_array_numeric() {
        let expr = parse("[1, 2, 3]").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::ScalarUDF {
            fun: Arc::new(array_constructor_udf()),
            args: vec![lit(1.0), lit(2.0), lit(3.0)],
        };
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();

        let expected_value = ScalarValue::List(
            Some(vec![
                ScalarValue::from(1.0),
                ScalarValue::from(2.0),
                ScalarValue::from(3.0),
            ]),
            Box::new(Field::new("item", DataType::Float64, true)),
        );

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_array_empty() {
        let expr = parse("[]").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::ScalarUDF {
            fun: Arc::new(array_constructor_udf()),
            args: vec![],
        };
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value. Empty array is given Float64 data type
        let result_value = result_expr.eval_to_scalar().unwrap();

        let expected_value = ScalarValue::List(
            Some(vec![]),
            Box::new(Field::new("item", DataType::Float64, true)),
        );

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_array_2d() {
        let expr = parse("[[1, 2], [3, 4], [5, 6]]").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::ScalarUDF {
            fun: Arc::new(array_constructor_udf()),
            args: vec![
                Expr::ScalarUDF {
                    fun: Arc::new(array_constructor_udf()),
                    args: vec![lit(1.0), lit(2.0)],
                },
                Expr::ScalarUDF {
                    fun: Arc::new(array_constructor_udf()),
                    args: vec![lit(3.0), lit(4.0)],
                },
                Expr::ScalarUDF {
                    fun: Arc::new(array_constructor_udf()),
                    args: vec![lit(5.0), lit(6.0)],
                },
            ],
        };
        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::List(
            Some(vec![
                ScalarValue::List(
                    Some(vec![ScalarValue::from(1.0), ScalarValue::from(2.0)]),
                    Box::new(Field::new("item", DataType::Float64, true)),
                ),
                ScalarValue::List(
                    Some(vec![ScalarValue::from(3.0), ScalarValue::from(4.0)]),
                    Box::new(Field::new("item", DataType::Float64, true)),
                ),
                ScalarValue::List(
                    Some(vec![ScalarValue::from(5.0), ScalarValue::from(6.0)]),
                    Box::new(Field::new("item", DataType::Float64, true)),
                ),
            ]),
            Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                true,
            )),
        );

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_compile_object() {
        let expr = parse("{a: 1, 'two': {three: 3}}").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::ScalarUDF {
            fun: Arc::new(make_object_constructor_udf(
                &["a".to_string(), "two".to_string()],
                &[
                    DataType::Float64,
                    DataType::Struct(vec![Field::new("three", DataType::Float64, false)]),
                ],
            )),
            args: vec![
                lit(1.0),
                Expr::ScalarUDF {
                    fun: Arc::new(make_object_constructor_udf(
                        &["three".to_string()],
                        &[DataType::Float64],
                    )),
                    args: vec![lit(3.0)],
                },
            ],
        };

        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected_value = ScalarValue::from(vec![
            ("a", ScalarValue::from(1.0)),
            (
                "two",
                ScalarValue::from(vec![("three", ScalarValue::from(3.0))]),
            ),
        ]);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected_value);
    }

    #[test]
    fn test_eval_object_member() {
        let expr = parse("({a: 1, 'two': 2})['tw' + 'o']").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Float64(Some(2.0));
        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_compile_datum_member() {
        let expr = parse("datum['tw' + 'o'] * 3").unwrap();
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "two",
            DataType::Float64,
            true,
        )]))
        .unwrap();

        let result_expr = compile(&expr, &Default::default(), Some(&schema)).unwrap();

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(col("two")),
            op: Operator::Multiply,
            right: Box::new(lit(3.0)),
        };

        println!("expr: {:?}", result_expr);
        assert_eq!(result_expr, expected_expr);
    }

    #[test]
    fn test_compile_datum_nested_member() {
        let expr = parse("datum['two'].foo * 3").unwrap();
        // let expr = parse("[datum['two'].foo * 3, datum['two'].foo]").unwrap();
        let foo_field = Field::new("foo", DataType::Float64, false);

        let two_type = DataType::Struct(vec![foo_field.clone()]);
        let two_field = Field::new("two", two_type, true);
        let schema = Schema::new(vec![two_field]);
        let schema = DFSchema::try_from(schema).unwrap();
        let result_expr = compile(&expr, &Default::default(), Some(&schema)).unwrap();
        println!("compiled: {:?}", result_expr);

        // Make some data
        let foo_array = Arc::new(Float64Array::from(vec![11.0, 22.0, 33.0])) as ArrayRef;
        let two_array = Arc::new(StructArray::from(vec![(foo_field, foo_array)])) as ArrayRef;
        let datum_rb = RecordBatch::try_from_iter(vec![("two", two_array)]).unwrap();
        let evaluated = result_expr.eval_to_column(&datum_rb).unwrap();

        match evaluated {
            ColumnarValue::Array(evaluated) => {
                println!("evaluated: {:?}", evaluated);
                let evaluated = evaluated.as_any().downcast_ref::<Float64Array>().unwrap();
                let evaluated: Vec<_> = evaluated.iter().map(|v| v.unwrap()).collect();
                assert_eq!(evaluated, vec![33.0, 66.0, 99.0])
            }
            ColumnarValue::Scalar(_) => {
                unreachable!()
            }
        }
    }

    #[test]
    fn test_eval_call_if() {
        let expr = parse("if(32, 7, 9)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();

        let expected_expr = Expr::Case {
            expr: None,
            when_then_expr: vec![(
                Box::new(and(
                    Expr::Cast {
                        expr: Box::new(lit(32.0)),
                        data_type: DataType::Boolean,
                    },
                    Expr::is_not_null(lit(32.0)),
                )),
                Box::new(lit(7.0)),
            )],
            else_expr: Some(Box::new(lit(9.0))),
        };
        assert_eq!(result_expr, expected_expr);
        println!("expr: {:?}", result_expr);

        // Check evaluated value
        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Float64(Some(7.0));
        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_call_abs() {
        let expr = parse("abs(-2)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Float64(Some(2.0));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_call_pow() {
        let expr = parse("pow(3, 4)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Float64(Some(81.0));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_call_round() {
        let expr = parse("round(4.8)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Float64(Some(5.0));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_call_is_nan() {
        let expr = parse("isNaN(NaN + 4)").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::Boolean(Some(true));

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_length() {
        let expr = parse("length([1, 2, 3])").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::from(3);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn test_eval_length_member() {
        let expr = parse("[1, 2, 3].length").unwrap();
        let result_expr = compile(&expr, &Default::default(), None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        let expected = ScalarValue::from(3);

        println!("value: {:?}", result_value);
        assert_eq!(result_value, expected);
    }

    #[test]
    fn try_datetime() {
        let expr = parse("datetime('2007-04-05T14:30:00')").unwrap();
        let config = CompilationConfig {
            tz_config: Some(RuntimeTzConfig {
                local_tz: chrono_tz::Tz::America__New_York,
                default_input_tz: chrono_tz::Tz::America__New_York,
            }),
            ..Default::default()
        };
        let result_expr = compile(&expr, &config, None).unwrap();
        println!("expr: {:?}", result_expr);

        let result_value = result_expr.eval_to_scalar().unwrap();
        println!("result_value: {:?}", result_value);
    }
}
