use datafusion_expr::{Expr, WindowFunction};
use sqlgen::ast::{Function as SqlFunction, FunctionArg, FunctionArgExpr, Ident, ObjectName, WindowSpec as SqlWindowSpec};
use vegafusion_core::error::{Result, VegaFusionError};
use crate::sql::compile::expr::ToSqlExpr;
use crate::sql::compile::order::ToSqlOrderByExpr;


pub trait ToSqlWindowFunction {
    fn to_sql_window(&self) -> Result<SqlFunction>;
}

impl ToSqlWindowFunction for Expr {
    fn to_sql_window(&self) -> Result<SqlFunction> {
        match self {
            Expr::WindowFunction { fun, args, partition_by, order_by, window_frame } => {
                // Extract function name
                let name_str = match fun {
                    WindowFunction::AggregateFunction(agg) => {
                        agg.to_string()
                    }
                    WindowFunction::BuiltInWindowFunction(win_fn) => {
                        win_fn.to_string()
                    }
                };

                // Process args
                let args = args.iter().map(|arg| {
                    Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(arg.to_sql()?)))
                }).collect::<Result<Vec<_>>>()?;
                let partition_by = partition_by.iter().map(|arg| arg.to_sql()).collect::<Result<Vec<_>>>()?;
                let order_by = order_by.iter().map(|arg| arg.to_sql_order()).collect::<Result<Vec<_>>>()?;

                if window_frame.is_some() {
                    return Err(VegaFusionError::internal("Window frame is not yet supported"))
                }

                // Process over
                let over = SqlWindowSpec {
                    partition_by,
                    order_by,
                    window_frame: None
                };

                Ok(SqlFunction {
                    name: ObjectName(vec![Ident { value: name_str, quote_style: None }]),
                    args,
                    over: Some(over),
                    distinct: false
                })
            }
            _ => Err(VegaFusionError::internal("Only Window expressions may be converted to SqlFunction AST nodes"))
        }
    }
}