/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::lexer::{tokenize, Token};
use crate::expression::ops::{binary_op_from_token, logical_op_from_token, unary_op_from_token};
use crate::proto::gen::expression::expression::Expr;

use crate::proto::gen::expression::{
    ArrayExpression, BinaryExpression, BinaryOperator, CallExpression, ConditionalExpression,
    Expression, Identifier, Literal, LogicalExpression, LogicalOperator, MemberExpression,
    ObjectExpression, Property, Span, UnaryExpression, UnaryOperator,
};

pub fn parse(expr: &str) -> Result<Expression> {
    let mut tokens = tokenize(expr)?;
    let result = perform_parse(&mut tokens, 0.0, expr)?;
    if !tokens.is_empty() {
        let (token, start, _) = &tokens[0];
        return Err(VegaFusionError::parse(&format!(
            "Unexpected token {} at position {} in expression: {}",
            token, start, expr
        )));
    }

    Ok(result)
}

fn perform_parse(
    tokens: &mut Vec<(Token, usize, usize)>,
    min_bp: f64,
    full_expr: &str,
) -> Result<Expression> {
    if tokens.is_empty() {
        return Err(VegaFusionError::parse("Unexpected end of expression"));
    }

    // Pop leading token
    let (lhs_token, start, end) = tokens[0].clone();
    tokens.remove(0);

    // parse form that starts with lhs_token
    let lhs_result = if is_atom(&lhs_token) {
        parse_atom(&lhs_token, start, end)
    } else if let Ok(op) = unary_op_from_token(&lhs_token) {
        // Unary expression
        parse_unary(tokens, op, start, full_expr)
    } else if lhs_token == Token::OpenParen {
        // Arbitrary expression inside parans
        parse_paren_grouping(tokens, full_expr)
    } else if lhs_token == Token::OpenSquare {
        // Array literal expression
        parse_array(tokens, start, full_expr)
    } else if lhs_token == Token::OpenCurly {
        // Object literal expression
        parse_object(tokens, start, full_expr)
    } else {
        Err(VegaFusionError::parse(&format!(
            "Unexpected token: {}",
            lhs_token
        )))
    };

    let mut lhs = lhs_result.with_context(|| {
        format!(
            "Failed to parse form starting at position {} in expression: {}",
            start, full_expr
        )
    })?;

    // pop tokens and add to lhs expression
    while !tokens.is_empty() {
        let (token, start, _) = &tokens[0];
        let start = *start;

        // Check for tokens that always close expressions. If found, break out of while loop
        match token {
            Token::CloseParen
            | Token::CloseCurly
            | Token::CloseSquare
            | Token::Comma
            | Token::Colon => break,
            _ => {}
        }

        let expr_result: Result<Expression> = if let Ok(op) = binary_op_from_token(token) {
            if let Some(new_lhs_result) = parse_binary(tokens, op, &lhs, min_bp, start, full_expr) {
                new_lhs_result
            } else {
                break;
            }
        } else if let Ok(op) = logical_op_from_token(token) {
            if let Some(new_lhs_result) = parse_logical(tokens, op, &lhs, min_bp, start, full_expr)
            {
                new_lhs_result
            } else {
                break;
            }
        } else if token == &Token::OpenParen {
            // Function call (e.g. foo(bar))
            if let Some(new_lhs_result) = parse_call(tokens, &lhs, min_bp, start, full_expr) {
                new_lhs_result
            } else {
                break;
            }
        } else if token == &Token::OpenSquare {
            // computed object/array membership (e.g. foo['bar'])
            if let Some(new_lhs_result) =
                parse_computed_member(tokens, &lhs, min_bp, start, full_expr)
            {
                new_lhs_result
            } else {
                break;
            }
        } else if token == &Token::Dot {
            // static property membership (e.g. foo.bar)
            if let Some(new_lhs_result) =
                parse_static_member(tokens, &lhs, min_bp, start, full_expr)
            {
                new_lhs_result
            } else {
                break;
            }
        } else if token == &Token::Question {
            // ternary operator (e.g. foo ? bar: baz)
            if let Some(new_lhs_result) = parse_ternary(tokens, &lhs, min_bp, start, full_expr) {
                new_lhs_result
            } else {
                break;
            }
        } else {
            Err(VegaFusionError::parse(&format!(
                "Unexpected token '{}'",
                token
            )))
        };

        lhs = expr_result.with_context(|| {
            format!(
                "Failed to parse form starting at position {} in expression: {}",
                start, full_expr
            )
        })?;
    }

    Ok(lhs)
}

pub fn expect_token(
    tokens: &mut Vec<(Token, usize, usize)>,
    expected: Token,
) -> Result<(Token, usize, usize)> {
    if tokens.is_empty() {
        return Err(VegaFusionError::parse(&format!(
            "Expected {}, reached end of expression",
            expected
        )));
    }
    let (token, start, end) = tokens[0].clone();
    if token != expected {
        return Err(VegaFusionError::parse(&format!(
            "Expected {}, received {}",
            expected, token
        )));
    }
    tokens.remove(0);
    Ok((token, start, end))
}

/// Check whether token is an atomic Expression
pub fn is_atom(token: &Token) -> bool {
    matches!(
        token,
        Token::Null
            | Token::Number { .. }
            | Token::Identifier { .. }
            | Token::String { .. }
            | Token::Bool { .. }
    )
}

/// Parse atom token to Expression
pub fn parse_atom(token: &Token, start: usize, end: usize) -> Result<Expression> {
    let span = Span {
        start: start as i32,
        end: end as i32,
    };

    let expr = match token {
        Token::Null => Expr::from(Literal::null()),
        Token::Bool { value, raw } => Expr::from(Literal::new(*value, raw)),
        Token::Number { value, raw } => Expr::from(Literal::new(*value, raw)),
        Token::String { value, raw } => Expr::from(Literal::new(value.clone(), raw)),
        Token::Identifier { value } => Expr::from(Identifier::new(value)),
        _ => {
            return Err(VegaFusionError::parse(&format!(
                "Token not an atom: {}",
                token
            )))
        }
    };

    Ok(Expression {
        expr: Some(expr),
        span: Some(span),
    })
}

pub fn parse_unary(
    tokens: &mut Vec<(Token, usize, usize)>,
    op: UnaryOperator,
    start: usize,
    full_expr: &str,
) -> Result<Expression> {
    let unary_bp = op.unary_binding_power();
    let rhs = perform_parse(tokens, unary_bp, full_expr)?;
    let new_span = Span {
        start: start as i32,
        end: rhs.span.clone().unwrap().end,
    };
    let expr = Expr::from(UnaryExpression::new(&op, rhs));
    Ok(Expression::new(expr, Some(new_span)))
}

pub fn parse_binary(
    tokens: &mut Vec<(Token, usize, usize)>,
    op: BinaryOperator,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    // Infix operator
    let (left_bp, right_bp) = op.infix_binding_power();
    if left_bp < min_bp {
        return None;
    }

    // Commit to processing operator token
    tokens.remove(0);

    Some(match perform_parse(tokens, right_bp, full_expr) {
        Ok(rhs) => {
            // Update lhs
            let new_span = Span {
                start: start as i32,
                end: rhs.span.clone().unwrap().end,
            };
            let expr = Expr::from(BinaryExpression::new(lhs.clone(), &op, rhs));
            Ok(Expression::new(expr, Some(new_span)))
        }
        Err(err) => Err(err),
    })
}

pub fn parse_logical(
    tokens: &mut Vec<(Token, usize, usize)>,
    op: LogicalOperator,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    // Infix operator
    let (left_bp, right_bp) = op.infix_binding_power();
    if left_bp < min_bp {
        return None;
    }
    // Commit to processing operator token
    tokens.remove(0);

    Some(match perform_parse(tokens, right_bp, full_expr) {
        Ok(rhs) => {
            // Update lhs
            let new_span = Span {
                start: start as i32,
                end: rhs.span.clone().unwrap().end,
            };
            let expr = Expr::from(LogicalExpression::new(lhs.clone(), &op, rhs));
            Ok(Expression::new(expr, Some(new_span)))
        }
        Err(err) => Err(err),
    })
}

pub fn parse_call(
    tokens: &mut Vec<(Token, usize, usize)>,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    let lhs = match lhs
        .as_identifier()
        .with_context(|| "Only global functions are callable")
    {
        Ok(identifier) => identifier,
        Err(err) => return Some(Err(err)),
    };

    // For precedence, see
    // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Operator_Precedence
    let computed_member_bp = 20.0;
    if min_bp >= computed_member_bp {
        return None;
    }

    // Opening paren
    expect_token(tokens, Token::OpenParen).unwrap();

    // Parse arguments
    let mut arguments: Vec<Expression> = Vec::new();
    while !tokens.is_empty() && tokens[0].0 != Token::CloseParen {
        let parsed_arg = perform_parse(tokens, 1.0, full_expr);
        match parsed_arg {
            Ok(parsed_arg) => {
                arguments.push(parsed_arg);

                // Remove comma token, if any
                expect_token(tokens, Token::Comma).ok();
            }
            Err(err) => return Some(Err(err)),
        }
    }

    // Closing paren
    let (_, _, end) = expect_token(tokens, Token::CloseParen).unwrap();

    // Update span
    let new_span = Span {
        start: start as i32,
        end: end as i32,
    };
    let expr = Expr::from(CallExpression::new(&lhs.name, arguments));
    Some(Ok(Expression::new(expr, Some(new_span))))
}

pub fn parse_computed_member(
    tokens: &mut Vec<(Token, usize, usize)>,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    let computed_member_bp = 20.0;
    if min_bp >= computed_member_bp {
        return None;
    }

    // Opening bracket
    expect_token(tokens, Token::OpenSquare).unwrap();

    // Property expression
    Some(match perform_parse(tokens, 1.0, full_expr) {
        Ok(property) => {
            // Closing bracket
            let (_, _, end) = expect_token(tokens, Token::CloseSquare).unwrap();

            // Update span
            let new_span = Span {
                start: start as i32,
                end: end as i32,
            };

            let expr = Expr::from(MemberExpression::new_computed(lhs.clone(), property));
            Ok(Expression::new(expr, Some(new_span)))
        }
        Err(err) => Err(err),
    })
}

pub fn parse_static_member(
    tokens: &mut Vec<(Token, usize, usize)>,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    let computed_member_bp = 20.0;
    if min_bp >= computed_member_bp {
        return None;
    }

    // Dot
    expect_token(tokens, Token::Dot).unwrap();

    // Property expression
    Some(match perform_parse(tokens, 1000.0, full_expr) {
        Ok(property) => {
            // Update span
            let new_span = Span {
                start: start as i32,
                end: property.span.clone().unwrap().end,
            };

            let expr = match MemberExpression::new_static(lhs.clone(), property) {
                Ok(member) => Expr::from(member),
                Err(err) => return Some(Err(err)),
            };
            Ok(Expression::new(expr, Some(new_span)))
        }
        Err(err) => Err(err),
    })
}

pub fn parse_ternary(
    tokens: &mut Vec<(Token, usize, usize)>,
    lhs: &Expression,
    min_bp: f64,
    start: usize,
    full_expr: &str,
) -> Option<Result<Expression>> {
    let (left_bp, middle_bp, right_bp) = ConditionalExpression::ternary_binding_power();
    if min_bp >= left_bp {
        return None;
    }

    // Question mark
    expect_token(tokens, Token::Question).unwrap();

    // Parse consequent
    let consequent = if let Ok(consequent) = perform_parse(tokens, middle_bp, full_expr) {
        consequent
    } else {
        return Some(Err(VegaFusionError::parse(
            "Failed to parse consequent of ternary operator",
        )));
    };

    // Colon
    expect_token(tokens, Token::Colon).unwrap();

    // Parse alternate
    let alternate = if let Ok(alternate) = perform_parse(tokens, right_bp, full_expr) {
        alternate
    } else {
        return Some(Err(VegaFusionError::parse(
            "Failed to parse alternate of ternary operator",
        )));
    };

    // Update span
    let new_span = Span {
        start: start as i32,
        end: alternate.span.clone().unwrap().end,
    };

    let expr = Expr::from(ConditionalExpression::new(
        lhs.clone(),
        consequent,
        alternate,
    ));
    Some(Ok(Expression::new(expr, Some(new_span))))
}

pub fn parse_paren_grouping(
    tokens: &mut Vec<(Token, usize, usize)>,
    full_expr: &str,
) -> Result<Expression> {
    perform_parse(tokens, 0.0, full_expr).and_then(|new_lhs| {
        expect_token(tokens, Token::CloseParen)?;
        Ok(new_lhs)
    })
}

pub fn parse_array(
    tokens: &mut Vec<(Token, usize, usize)>,
    start: usize,
    full_expr: &str,
) -> Result<Expression> {
    let mut elements: Vec<Expression> = Vec::new();

    while !tokens.is_empty() && tokens[0].0 != Token::CloseSquare {
        elements.push(perform_parse(tokens, 1.0, full_expr)?);

        // Remove single comma token, if any
        expect_token(tokens, Token::Comma).ok();
    }

    // Closing bracket
    let (_, _, end) = expect_token(tokens, Token::CloseSquare).unwrap();

    // Update span
    let new_span = Span {
        start: start as i32,
        end: end as i32,
    };

    let expr = Expr::from(ArrayExpression::new(elements));
    Ok(Expression::new(expr, Some(new_span)))
}

pub fn parse_object(
    tokens: &mut Vec<(Token, usize, usize)>,
    start: usize,
    full_expr: &str,
) -> Result<Expression> {
    let mut properties: Vec<Property> = Vec::new();

    while !tokens.is_empty() && tokens[0].0 != Token::CloseCurly {
        let key = match perform_parse(tokens, 1.0, full_expr) {
            Ok(key) => key,
            Err(err) => return Err(err.with_context(|| "Failed to parse object key".to_string())),
        };

        expect_token(tokens, Token::Colon)?;

        let value = match perform_parse(tokens, 1.0, full_expr) {
            Ok(key) => key,
            Err(err) => {
                return Err(err.with_context(|| "Failed to parse object property value".to_string()))
            }
        };

        // Remove comma token, if any
        expect_token(tokens, Token::Comma).ok();

        let property = Property::try_new(key, value)?;
        properties.push(property);
    }

    // Closing bracket
    let (_, _, end) = expect_token(tokens, Token::CloseCurly).unwrap();

    // Update span
    let new_span = Span {
        start: start as i32,
        end: end as i32,
    };

    let expr = Expr::from(ObjectExpression::new(properties));
    Ok(Expression::new(expr, Some(new_span)))
}

#[cfg(test)]
mod test_parse {
    use crate::expression::parser::parse;

    #[test]
    fn test_parse_atom() {
        let node = parse("23.500000").unwrap();
        assert_eq!(format!("{}", node), "23.5");

        let node = parse("\"hello\"").unwrap();
        assert_eq!(format!("{}", node), "\"hello\"");
    }

    #[test]
    fn test_parse_binary() {
        let node = parse("23.50 + foo * 87").unwrap();
        assert_eq!(node.to_string(), "23.5 + foo * 87");
    }

    #[test]
    fn test_parse_logical() {
        let node = parse("false || (foo && bar)").unwrap();
        assert_eq!(node.to_string(), "false || foo && bar");
    }

    #[test]
    fn test_parse_prefix() {
        let node = parse("-23.50 + +foo").unwrap();
        assert_eq!(node.to_string(), "-23.5 + +foo");
    }

    #[test]
    fn test_paren_grouping() {
        let node = parse("-(23.50 + foo)").unwrap();
        assert_eq!(node.to_string(), "-(23.5 + foo)");
    }

    #[test]
    fn test_call() {
        // One arg
        let node = parse("foo(19.0)").unwrap();
        assert_eq!(node.to_string(), "foo(19)");

        // Zero args
        let node = parse("foo()").unwrap();
        assert_eq!(node.to_string(), "foo()");

        // Two args
        let node = parse("foo('a', 21)").unwrap();
        assert_eq!(node.to_string(), "foo(\"a\", 21)");

        // Two args, trailing comma
        let node = parse("foo('a', 21,)").unwrap();
        assert_eq!(node.to_string(), "foo(\"a\", 21)");
    }

    #[test]
    fn test_computed_membership() {
        let node = parse("foo[19.0]").unwrap();
        assert_eq!(node.to_string(), "foo[19]");

        let node = parse("foo['bar']").unwrap();
        assert_eq!(node.to_string(), "foo[\"bar\"]");
    }

    #[test]
    fn test_static_membership() {
        let node = parse("foo.bar").unwrap();
        assert_eq!(node.to_string(), "foo.bar");

        let node = parse("foo.bar[2]").unwrap();
        assert_eq!(node.to_string(), "foo.bar[2]");
    }

    #[test]
    fn test_ternary() {
        let node = parse("foo ? 2 + 3: 27").unwrap();
        assert_eq!(node.to_string(), "foo ? 2 + 3: 27");

        let node = parse("foo ? 2 + 3: 27 || 17").unwrap();
        assert_eq!(node.to_string(), "foo ? 2 + 3: 27 || 17");

        let node = parse("foo ? 2 + 3: (27 || 17)").unwrap();
        assert_eq!(node.to_string(), "foo ? 2 + 3: 27 || 17");

        let node = parse("(foo ? 2 + 3: 27) || 17").unwrap();
        assert_eq!(node.to_string(), "(foo ? 2 + 3: 27) || 17");

        // Check right associativity
        let node = parse("c1 ? v1: c2 ? v2: c3 ? v3: v4").unwrap();
        assert_eq!(node.to_string(), "c1 ? v1: c2 ? v2: c3 ? v3: v4");

        let node = parse("c1 ? v1: (c2 ? v2: (c3 ? v3: v4))").unwrap();
        assert_eq!(node.to_string(), "c1 ? v1: c2 ? v2: c3 ? v3: v4");

        let node = parse("((c1 ? v1: c2) ? v2: c3)? v3: v4").unwrap();
        assert_eq!(node.to_string(), "((c1 ? v1: c2) ? v2: c3) ? v3: v4");
    }

    #[test]
    fn test_array() {
        let node = parse("[19.0]").unwrap();
        assert_eq!(node.to_string(), "[19]");

        let node = parse("['bar', 23]").unwrap();
        assert_eq!(node.to_string(), "[\"bar\", 23]");

        let node = parse("[]").unwrap();
        assert_eq!(node.to_string(), "[]");
    }

    #[test]
    fn test_object() {
        let node = parse("{a: 2, 'b': 2 + 2}").unwrap();
        assert_eq!(node.to_string(), r#"{a: 2, "b": 2 + 2}"#);
    }
}
