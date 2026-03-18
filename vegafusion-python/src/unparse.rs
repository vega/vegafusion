use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use datafusion::prelude::SessionContext;
use datafusion_proto::generated::datafusion::LogicalExprNode;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_sql::unparser::dialect::{
    BigQueryDialect, DefaultDialect, Dialect, DuckDBDialect, MySqlDialect, PostgreSqlDialect,
    SqliteDialect,
};
use datafusion_sql::unparser::Unparser;
use prost::Message;
use vegafusion_common::datafusion_expr::Expr;
use vegafusion_runtime::data::codec::VegaFusionCodec;

fn make_dialect(dialect: &str) -> PyResult<Box<dyn Dialect>> {
    match dialect {
        "default" => Ok(Box::new(DefaultDialect {})),
        "postgres" | "postgresql" => Ok(Box::new(PostgreSqlDialect {})),
        "mysql" => Ok(Box::new(MySqlDialect {})),
        "sqlite" => Ok(Box::new(SqliteDialect {})),
        "duckdb" => Ok(Box::new(DuckDBDialect::new())),
        "bigquery" => Ok(Box::new(BigQueryDialect {})),
        _ => Err(PyValueError::new_err(format!(
            "Unknown dialect '{dialect}'. Supported: default, postgres, mysql, sqlite, duckdb, bigquery"
        ))),
    }
}

/// Convert a protobuf-serialized LogicalPlan to a SQL string.
///
/// Args:
///     plan_bytes: Serialized LogicalPlanNode protobuf bytes.
///     dialect: SQL dialect name. One of "default", "postgres", "mysql",
///              "sqlite", "duckdb", "bigquery".
///
/// Returns:
///     The SQL string representation of the plan.
#[pyfunction]
#[pyo3(signature = (plan_bytes, dialect="default"))]
pub fn unparse_plan_to_sql(plan_bytes: Vec<u8>, dialect: &str) -> PyResult<String> {
    let codec = VegaFusionCodec::new();
    let ctx = SessionContext::new();
    let plan = datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec(
        &plan_bytes,
        &ctx.task_ctx(),
        &codec,
    )
    .map_err(|e| PyValueError::new_err(format!("Failed to deserialize plan: {e}")))?;

    let d = make_dialect(dialect)?;
    let sql = Unparser::new(d.as_ref())
        .plan_to_sql(&plan)
        .map_err(|e| PyValueError::new_err(format!("Failed to unparse plan to SQL: {e}")))?;

    Ok(sql.to_string())
}

/// Convert protobuf-serialized filter expressions to a SQL WHERE clause string.
///
/// Accepts a single expression or a list of expressions (joined with AND).
///
/// Args:
///     expr_bytes: A single serialized LogicalExprNode (bytes) or a list of them.
///     dialect: SQL dialect name. One of "default", "postgres", "mysql",
///              "sqlite", "duckdb", "bigquery".
///
/// Returns:
///     The SQL string representation of the expression(s).
#[pyfunction]
#[pyo3(signature = (expr_bytes, dialect="default"))]
pub fn unparse_expr_to_sql(expr_bytes: Vec<Vec<u8>>, dialect: &str) -> PyResult<String> {
    if expr_bytes.is_empty() {
        return Err(PyValueError::new_err(
            "expr_bytes must contain at least one expression",
        ));
    }

    let ctx = SessionContext::new();
    let codec = VegaFusionCodec::new();

    let exprs: Vec<Expr> = expr_bytes
        .iter()
        .map(|bytes| {
            let proto = LogicalExprNode::decode(bytes.as_slice()).map_err(|e| {
                PyValueError::new_err(format!("Failed to decode LogicalExprNode: {e}"))
            })?;
            parse_expr(&proto, &ctx, &codec)
                .map_err(|e| PyValueError::new_err(format!("Failed to parse expression: {e}")))
        })
        .collect::<PyResult<Vec<_>>>()?;

    // Join multiple expressions with AND
    let combined = exprs
        .into_iter()
        .reduce(|a, b| a.and(b))
        .expect("non-empty after validation");

    let d = make_dialect(dialect)?;
    let sql_expr = Unparser::new(d.as_ref())
        .expr_to_sql(&combined)
        .map_err(|e| PyValueError::new_err(format!("Failed to unparse expression to SQL: {e}")))?;

    Ok(sql_expr.to_string())
}
