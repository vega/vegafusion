use crate::compile::data_type::ToSqlDataType;
use crate::compile::scalar::ToSqlScalar;
use arrow::datatypes::DataType;
use datafusion_common::{DFSchema, ScalarValue};
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, CastKind, DuplicateTreatment, Expr as SqlExpr,
    Function as SqlFunction, FunctionArg as SqlFunctionArg, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName as SqlObjectName, UnaryOperator as SqlUnaryOperator,
    WindowFrame as SqlWindowFrame, WindowFrameBound as SqlWindowBound,
    WindowFrameUnits as SqlWindowFrameUnits, WindowSpec as SqlWindowSpec, WindowType,
};

use datafusion_expr::expr::{BinaryExpr, Case, Cast, Sort};
use datafusion_expr::{
    expr, lit, Between, BuiltInWindowFunction, Expr, ExprSchemable, Operator, WindowFrameBound,
    WindowFrameUnits, WindowFunctionDefinition,
};

use crate::compile::function_arg::ToSqlFunctionArg;
use crate::compile::order::ToSqlOrderByExpr;
use crate::dialect::{Dialect, TryCastMode, UnorderedRowNumberMode};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::error::{Result, VegaFusionError};

pub trait ToSqlExpr {
    fn to_sql(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr>;
}

impl ToSqlExpr for Expr {
    fn to_sql(&self, dialect: &Dialect, schema: &DFSchema) -> Result<SqlExpr> {
        match self {
            Expr::Alias(_) => {
                // Alias expressions need to be handled at a higher level
                Err(VegaFusionError::internal(format!(
                    "Alias cannot be converted to SQL: {self:?}"
                )))
            }
            Expr::Column(col) => {
                let id = match &col.relation {
                    Some(relation) => SqlExpr::CompoundIdentifier(vec![
                        Ident::with_quote(dialect.quote_style, relation.to_string()),
                        Ident::with_quote(dialect.quote_style, &col.name),
                    ]),
                    None => SqlExpr::Identifier(Ident::with_quote(dialect.quote_style, &col.name)),
                };
                Ok(id)
            }
            Expr::ScalarVariable(_, _) => Err(VegaFusionError::internal(
                "ScalarVariable cannot be converted to SQL",
            )),
            Expr::Literal(value) => Ok(value.to_sql(dialect)?),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if dialect.binary_ops.contains(op) {
                    let sql_op = match op {
                        Operator::Eq => SqlBinaryOperator::Eq,
                        Operator::NotEq => SqlBinaryOperator::NotEq,
                        Operator::Lt => SqlBinaryOperator::Lt,
                        Operator::LtEq => SqlBinaryOperator::LtEq,
                        Operator::Gt => SqlBinaryOperator::Gt,
                        Operator::GtEq => SqlBinaryOperator::GtEq,
                        Operator::Plus => SqlBinaryOperator::Plus,
                        Operator::Minus => SqlBinaryOperator::Minus,
                        Operator::Multiply => SqlBinaryOperator::Multiply,
                        Operator::Divide => SqlBinaryOperator::Divide,
                        Operator::Modulo => SqlBinaryOperator::Modulo,
                        Operator::And => SqlBinaryOperator::And,
                        Operator::Or => SqlBinaryOperator::Or,
                        Operator::IsDistinctFrom => {
                            return Err(VegaFusionError::internal(
                                "IsDistinctFrom cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::IsNotDistinctFrom => {
                            return Err(VegaFusionError::internal(
                                "IsNotDistinctFrom cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::RegexMatch => SqlBinaryOperator::PGRegexMatch,
                        Operator::RegexIMatch => SqlBinaryOperator::PGRegexIMatch,
                        Operator::RegexNotMatch => SqlBinaryOperator::PGRegexNotMatch,
                        Operator::RegexNotIMatch => SqlBinaryOperator::PGRegexNotIMatch,
                        Operator::BitwiseAnd => SqlBinaryOperator::BitwiseAnd,
                        Operator::BitwiseOr => SqlBinaryOperator::BitwiseOr,
                        Operator::BitwiseXor => SqlBinaryOperator::BitwiseXor,
                        Operator::StringConcat => SqlBinaryOperator::StringConcat,
                        Operator::BitwiseShiftRight => SqlBinaryOperator::PGBitwiseShiftRight,
                        Operator::BitwiseShiftLeft => SqlBinaryOperator::PGBitwiseShiftLeft,
                        Operator::AtArrow => {
                            return Err(VegaFusionError::internal(
                                "AtArrow cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::ArrowAt => {
                            return Err(VegaFusionError::internal(
                                "ArrowAt cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::LikeMatch => {
                            return Err(VegaFusionError::internal(
                                "LikeMatch cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::ILikeMatch => {
                            return Err(VegaFusionError::internal(
                                "ILikeMatch cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::NotLikeMatch => {
                            return Err(VegaFusionError::internal(
                                "NotLikeMatch cannot be converted to SQL".to_string(),
                            ))
                        }
                        Operator::NotILikeMatch => {
                            return Err(VegaFusionError::internal(
                                "NotILikeMatch cannot be converted to SQL".to_string(),
                            ))
                        }
                    };
                    Ok(SqlExpr::Nested(Box::new(SqlExpr::BinaryOp {
                        left: Box::new(left.to_sql(dialect, schema)?),
                        op: sql_op,
                        right: Box::new(right.to_sql(dialect, schema)?),
                    })))
                } else if let Some(transformer) = dialect.binary_op_transforms.get(op) {
                    transformer.transform(
                        op,
                        left.to_sql(dialect, schema)?,
                        right.to_sql(dialect, schema)?,
                        dialect,
                    )
                } else {
                    return Err(VegaFusionError::sql_not_supported(format!(
                        "Dialect does not support the '{op:?}' operator"
                    )));
                }
            }
            Expr::Not(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Not,
                expr: Box::new(expr.to_sql(dialect, schema)?),
            }))),
            Expr::IsNotNull(expr) => {
                Ok(SqlExpr::IsNotNull(Box::new(expr.to_sql(dialect, schema)?)))
            }
            Expr::IsNull(expr) => Ok(SqlExpr::IsNull(Box::new(expr.to_sql(dialect, schema)?))),
            Expr::Negative(expr) => Ok(SqlExpr::Nested(Box::new(SqlExpr::UnaryOp {
                op: SqlUnaryOperator::Minus,
                expr: Box::new(expr.to_sql(dialect, schema)?),
            }))),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => Ok(SqlExpr::Between {
                expr: Box::new(expr.to_sql(dialect, schema)?),
                negated: *negated,
                low: Box::new(low.to_sql(dialect, schema)?),
                high: Box::new(high.to_sql(dialect, schema)?),
            }),
            Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            }) => {
                let (conditions, results): (Vec<Box<Expr>>, Vec<Box<Expr>>) =
                    when_then_expr.iter().cloned().unzip();

                let conditions = conditions
                    .iter()
                    .map(|expr| expr.to_sql(dialect, schema))
                    .collect::<Result<Vec<_>>>()?;
                let results = results
                    .iter()
                    .map(|expr| expr.to_sql(dialect, schema))
                    .collect::<Result<Vec<_>>>()?;

                let else_result = if let Some(else_expr) = &else_expr {
                    Some(Box::new(else_expr.to_sql(dialect, schema)?))
                } else {
                    None
                };

                Ok(SqlExpr::Case {
                    operand: if let Some(expr) = &expr {
                        Some(Box::new(expr.to_sql(dialect, schema)?))
                    } else {
                        None
                    },
                    conditions,
                    results,
                    else_result,
                })
            }
            Expr::Cast(Cast { expr, data_type }) => {
                // Build cast expression
                let from_dtype = expr.get_type(schema)?;
                let cast_expr = if let Some(transformer) = dialect
                    .cast_transformers
                    .get(&(from_dtype, data_type.clone()))
                {
                    transformer.transform(expr.as_ref(), dialect, schema)?
                } else {
                    let sql_data_type = data_type.to_sql(dialect)?;
                    SqlExpr::Cast {
                        expr: Box::new(expr.to_sql(dialect, schema)?),
                        data_type: sql_data_type,
                        format: None,
                        kind: CastKind::Cast,
                    }
                };

                // Handle manual null propagation
                Ok(if dialect.cast_propagates_null {
                    cast_expr
                } else {
                    // Need to manually propagate nulls through cast
                    let condition = Expr::IsNotNull(expr.clone()).to_sql(dialect, schema)?;
                    let result = cast_expr;
                    let else_result = lit(ScalarValue::Null).to_sql(dialect, schema)?;
                    SqlExpr::Case {
                        operand: None,
                        conditions: vec![condition],
                        results: vec![result],
                        else_result: Some(Box::new(else_result)),
                    }
                })
            }
            Expr::TryCast(expr::TryCast { expr, data_type }) => {
                let from_dtype = expr.get_type(schema)?;
                let sql_data_type = data_type.to_sql(dialect)?;
                let cast_expr = if let Some(transformer) = dialect
                    .cast_transformers
                    .get(&(from_dtype.clone(), data_type.clone()))
                {
                    // Cast transformer overrides TryCast as well as Cast
                    transformer.transform(expr.as_ref(), dialect, schema)?
                } else {
                    match &dialect.try_cast_mode {
                        TryCastMode::Supported => SqlExpr::Cast {
                            expr: Box::new(expr.to_sql(dialect, schema)?),
                            data_type: sql_data_type,
                            format: None,
                            kind: CastKind::TryCast,
                        },
                        TryCastMode::JustUseCast => SqlExpr::Cast {
                            expr: Box::new(expr.to_sql(dialect, schema)?),
                            data_type: sql_data_type,
                            format: None,
                            kind: CastKind::Cast,
                        },
                        TryCastMode::SafeCast => SqlExpr::Cast {
                            expr: Box::new(expr.to_sql(dialect, schema)?),
                            data_type: sql_data_type,
                            format: None,
                            kind: CastKind::SafeCast,
                        },
                        TryCastMode::SupportedOnStringsOtherwiseJustCast => {
                            if let DataType::Utf8 | DataType::LargeUtf8 = from_dtype {
                                // TRY_CAST is supported
                                SqlExpr::Cast {
                                    expr: Box::new(expr.to_sql(dialect, schema)?),
                                    data_type: sql_data_type,
                                    format: None,
                                    kind: CastKind::TryCast,
                                }
                            } else {
                                // Fall back to regular CAST
                                SqlExpr::Cast {
                                    expr: Box::new(expr.to_sql(dialect, schema)?),
                                    data_type: sql_data_type,
                                    format: None,
                                    kind: CastKind::Cast,
                                }
                            }
                        }
                    }
                };

                // Handle manual null propagation
                Ok(if dialect.cast_propagates_null {
                    cast_expr
                } else {
                    // Need to manually propagate nulls through cast
                    let condition = Expr::IsNotNull(expr.clone()).to_sql(dialect, schema)?;
                    let result = cast_expr;
                    let else_result = lit(ScalarValue::Null).to_sql(dialect, schema)?;
                    SqlExpr::Case {
                        operand: None,
                        conditions: vec![condition],
                        results: vec![result],
                        else_result: Some(Box::new(else_result)),
                    }
                })
            }
            Expr::Sort { .. } => {
                // Sort expressions need to be handled at a higher level
                Err(VegaFusionError::internal("Sort cannot be converted to SQL"))
            }
            Expr::ScalarFunction(fun) => {
                let fun_name = match fun.name().to_ascii_lowercase().as_str() {
                    "power" => "pow".to_string(),
                    fun_name => fun_name.to_string(),
                };
                translate_scalar_function(&fun_name, &fun.args, dialect, schema)
            }
            Expr::AggregateFunction(expr::AggregateFunction {
                func,
                args,
                distinct,
                ..
            }) => translate_aggregate_function(
                &func.name().to_ascii_lowercase(),
                args.as_slice(),
                *distinct,
                dialect,
                schema,
            ),
            Expr::WindowFunction(expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment: _,
            }) => {
                // Extract function name
                let (fun_name, supports_frame) = match fun {
                    WindowFunctionDefinition::AggregateUDF(agg) => {
                        (agg.name().to_ascii_lowercase(), true)
                    }
                    WindowFunctionDefinition::BuiltInWindowFunction(win_fn) => {
                        let is_navigation_function = matches!(
                            win_fn,
                            BuiltInWindowFunction::FirstValue
                                | BuiltInWindowFunction::LastValue
                                | BuiltInWindowFunction::NthValue
                        );
                        let supports_frame = if is_navigation_function {
                            // Window frames sometimes supported by navigation functions like
                            // first_value.
                            dialect.supports_frames_in_navigation_window_functions
                        } else {
                            // Window frames sometimes supported by numbering functions like
                            // row_number, rank, etc.
                            dialect.supports_frames_in_numbering_window_functions
                        };

                        (win_fn.to_string().to_ascii_lowercase(), supports_frame)
                    }
                    WindowFunctionDefinition::WindowUDF(udf) => {
                        (udf.name().to_ascii_lowercase(), true)
                    }
                };

                // Handle unordered row_number
                let order_by = if fun_name == "row_number" && order_by.is_empty() {
                    match &dialect.unordered_row_number_mode {
                        UnorderedRowNumberMode::AlternateScalarFunction(alt_fun) => {
                            return Ok(SqlExpr::Function(SqlFunction {
                                name: SqlObjectName(vec![Ident::new(alt_fun)]),
                                args: FunctionArguments::List(FunctionArgumentList {
                                    args: vec![],
                                    duplicate_treatment: None,
                                    clauses: vec![],
                                }),
                                filter: None,
                                null_treatment: None,
                                over: None,
                                within_group: vec![],
                                parameters: FunctionArguments::None,
                            }));
                        }
                        UnorderedRowNumberMode::OrderByConstant => {
                            vec![Expr::Sort(Sort {
                                expr: Box::new(lit(1)),
                                asc: false,
                                nulls_first: false,
                            })]
                        }
                        _ => order_by.clone(),
                    }
                } else {
                    order_by.clone()
                };

                if dialect.aggregate_functions.contains(&fun_name)
                    || dialect.window_functions.contains(&fun_name)
                {
                    // Process args
                    let args = translate_function_args(args.as_slice(), dialect, schema)?;

                    let partition_by = partition_by
                        .iter()
                        .map(|arg| arg.to_sql(dialect, schema))
                        .collect::<Result<Vec<_>>>()?;

                    let order_by = order_by
                        .iter()
                        .map(|arg| arg.to_sql_order(dialect, schema))
                        .collect::<Result<Vec<_>>>()?;

                    let sql_window_frame = if supports_frame {
                        let end_bound =
                            compile_window_frame_bound(&window_frame.end_bound, dialect, schema)?;
                        let start_bound =
                            compile_window_frame_bound(&window_frame.start_bound, dialect, schema)?;

                        if !dialect.supports_bounded_window_frames
                            && (!matches!(start_bound, SqlWindowBound::Preceding(None))
                                || !matches!(end_bound, SqlWindowBound::CurrentRow))
                        {
                            // Found bounded window frame, which is not supported by dialect
                            return Err(VegaFusionError::sql_not_supported(
                                "Dialect does not support bounded window frames",
                            ));
                        }

                        let units = match window_frame.units {
                            WindowFrameUnits::Rows => SqlWindowFrameUnits::Rows,
                            WindowFrameUnits::Range => SqlWindowFrameUnits::Range,
                            WindowFrameUnits::Groups => {
                                if dialect.supports_window_frame_groups {
                                    SqlWindowFrameUnits::Groups
                                } else {
                                    return Err(VegaFusionError::sql_not_supported(
                                        "Dialect does not support window frame GROUPS",
                                    ));
                                }
                            }
                        };
                        Some(SqlWindowFrame {
                            units,
                            start_bound,
                            end_bound: Some(end_bound),
                        })
                    } else {
                        None
                    };

                    // Process over
                    let over = WindowType::WindowSpec(SqlWindowSpec {
                        partition_by,
                        order_by,
                        window_frame: sql_window_frame,
                        window_name: None,
                    });

                    let sql_fun = SqlFunction {
                        name: SqlObjectName(vec![Ident {
                            value: fun_name,
                            quote_style: None,
                        }]),
                        args: FunctionArguments::List(FunctionArgumentList {
                            args,
                            duplicate_treatment: None,
                            clauses: vec![],
                        }),
                        filter: None,
                        null_treatment: None,
                        over: Some(over),
                        within_group: vec![],
                        parameters: FunctionArguments::None,
                    };

                    Ok(SqlExpr::Function(sql_fun))
                } else {
                    // Unsupported
                    Err(VegaFusionError::sql_not_supported(format!(
                        "Dialect does not support the '{fun_name}' window function"
                    )))
                }
            }
            Expr::IsTrue(_) => Err(VegaFusionError::internal(
                "IsTrue cannot be converted to SQL",
            )),
            Expr::IsFalse(_) => Err(VegaFusionError::internal(
                "IsFalse cannot be converted to SQL",
            )),
            Expr::IsUnknown(_) => Err(VegaFusionError::internal(
                "IsUnknown cannot be converted to SQL",
            )),
            Expr::IsNotTrue(_) => Err(VegaFusionError::internal(
                "IsNotTrue cannot be converted to SQL",
            )),
            Expr::IsNotFalse(_) => Err(VegaFusionError::internal(
                "IsNotFalse cannot be converted to SQL",
            )),
            Expr::IsNotUnknown(_) => Err(VegaFusionError::internal(
                "IsNotUnknown cannot be converted to SQL",
            )),
            Expr::InList(expr::InList {
                expr,
                list,
                negated,
            }) => {
                let sql_expr = expr.to_sql(dialect, schema)?;
                let sql_list = list
                    .iter()
                    .map(|expr| expr.to_sql(dialect, schema))
                    .collect::<Result<Vec<_>>>()?;

                Ok(SqlExpr::InList {
                    expr: Box::new(sql_expr),
                    list: sql_list,
                    negated: *negated,
                })
            }
            Expr::Wildcard { .. } => Err(VegaFusionError::internal(
                "Wildcard cannot be converted to SQL",
            )),
            Expr::Exists { .. } => Err(VegaFusionError::internal(
                "Exists cannot be converted to SQL",
            )),
            Expr::InSubquery { .. } => Err(VegaFusionError::internal(
                "InSubquery cannot be converted to SQL",
            )),
            Expr::ScalarSubquery(_) => Err(VegaFusionError::internal(
                "ScalarSubquery cannot be converted to SQL",
            )),
            Expr::GroupingSet(_) => Err(VegaFusionError::internal(
                "GroupingSet cannot be converted to SQL",
            )),
            Expr::Like { .. } => Err(VegaFusionError::internal("Like cannot be converted to SQL")),
            Expr::SimilarTo { .. } => Err(VegaFusionError::internal(
                "SimilarTo cannot be converted to SQL",
            )),
            Expr::Placeholder { .. } => Err(VegaFusionError::internal(
                "Placeholder cannot be converted to SQL",
            )),
            Expr::OuterReferenceColumn(_, _) => Err(VegaFusionError::internal(
                "OuterReferenceColumn cannot be converted to SQL",
            )),
            Expr::Unnest(_) => Err(VegaFusionError::internal(
                "Unnest cannot be converted to SQL",
            )),
        }
    }
}

fn translate_scalar_function(
    fun_name: &str,
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<SqlExpr> {
    if dialect.scalar_functions.contains(fun_name) {
        // Function is directly supported by dialect
        let ident = Ident {
            value: fun_name.to_string(),
            quote_style: None,
        };
        let args = translate_function_args(args, dialect, schema)?;

        Ok(SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![ident]),
            args: FunctionArguments::List(FunctionArgumentList {
                args,
                duplicate_treatment: None,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        }))
    } else if let Some(transformer) = dialect.scalar_transformers.get(fun_name) {
        // Supported through AST transformation
        transformer.transform(args, dialect, schema)
    } else {
        // Unsupported
        return Err(VegaFusionError::sql_not_supported(format!(
            "Dialect does not support the '{fun_name}' scalar function"
        )));
    }
}

fn translate_aggregate_function(
    fun_name: &str,
    args: &[Expr],
    distinct: bool,
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<SqlExpr> {
    if dialect.aggregate_functions.contains(fun_name) {
        let ident = Ident {
            value: fun_name.to_ascii_lowercase(),
            quote_style: None,
        };
        let args = translate_function_args(args, dialect, schema)?;
        let fn_expr = SqlExpr::Function(SqlFunction {
            name: SqlObjectName(vec![ident]),
            args: FunctionArguments::List(FunctionArgumentList {
                args,
                duplicate_treatment: if distinct {
                    Some(DuplicateTreatment::Distinct)
                } else {
                    None
                },
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: FunctionArguments::None,
        });
        Ok(fn_expr)
    } else if let Some(transformer) = dialect.aggregate_transformers.get(fun_name) {
        // Supported through AST transformation
        transformer.transform(args, dialect, schema)
    } else {
        // Unsupported
        return Err(VegaFusionError::sql_not_supported(format!(
            "Dialect does not support the '{fun_name}' aggregate function"
        )));
    }
}

fn translate_function_args(
    args: &[Expr],
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<Vec<SqlFunctionArg>> {
    args.iter()
        .map(|expr| {
            Ok(SqlFunctionArg::Unnamed(
                expr.to_sql_function_arg(dialect, schema)?,
            ))
        })
        .collect::<Result<Vec<_>>>()
}

fn compile_window_frame_bound(
    bound: &WindowFrameBound,
    dialect: &Dialect,
    schema: &DFSchema,
) -> Result<SqlWindowBound> {
    Ok(match bound {
        WindowFrameBound::Preceding(v) => match v.to_f64() {
            Ok(v) => SqlWindowBound::Preceding(Some(Box::new(
                lit(v.max(0.0) as u64).to_sql(dialect, schema)?,
            ))),
            Err(_) => SqlWindowBound::Preceding(None),
        },
        WindowFrameBound::CurrentRow => SqlWindowBound::CurrentRow,
        WindowFrameBound::Following(v) => match v.to_f64() {
            Ok(v) => SqlWindowBound::Following(Some(Box::new(
                lit(v.max(0.0) as u64).to_sql(dialect, schema)?,
            ))),
            Err(_) => SqlWindowBound::Following(None),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::ToSqlExpr;
    use crate::dialect::Dialect;
    use arrow::datatypes::DataType;
    use datafusion_common::DFSchema;
    use datafusion_expr::expr::Cast;
    use datafusion_expr::{lit, Between, Expr};
    use datafusion_functions::expr_fn::sin;
    use datafusion_functions::string::expr_fn::upper;
    use vegafusion_common::column::flat_col;

    fn schema() -> DFSchema {
        DFSchema::empty()
    }

    #[test]
    pub fn test1() {
        let df_expr = Expr::Negative(Box::new(flat_col("A"))) + lit(12);
        let sql_expr = df_expr.to_sql(&Dialect::datafusion(), &schema()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"((-"A") + 12)"#.to_string());
    }

    #[test]
    pub fn test2() {
        let df_expr = sin(lit(1.2)) + flat_col("B");

        let dialect: Dialect = Dialect::datafusion();
        let sql_expr = df_expr.to_sql(&dialect, &schema()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"(sin(1.2) + "B")"#.to_string());
    }

    #[test]
    pub fn test3() {
        let df_expr = upper(lit("foo"));
        let dialect: Dialect = Dialect::datafusion();
        let sql_expr = df_expr.to_sql(&dialect, &schema()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "upper('foo')".to_string());
    }

    #[test]
    pub fn test4() {
        let df_expr = Expr::Cast(Cast {
            expr: Box::new(lit(2.8)),
            data_type: DataType::Int64,
        }) + lit(4);

        let sql_expr = df_expr.to_sql(&Dialect::datafusion(), &schema()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, "(CAST(2.8 AS BIGINT) + 4)".to_string());
    }

    #[test]
    pub fn test5() {
        let df_expr = Expr::Between(Between {
            expr: Box::new(flat_col("A")),
            negated: false,
            low: Box::new(lit(0)),
            high: Box::new(lit(10)),
        })
        .or(flat_col("B"));

        let sql_expr = df_expr.to_sql(&Dialect::datafusion(), &schema()).unwrap();
        println!("{sql_expr:?}");
        let sql_str = sql_expr.to_string();
        assert_eq!(sql_str, r#"("A" BETWEEN 0 AND 10 OR "B")"#.to_string());
    }
}
