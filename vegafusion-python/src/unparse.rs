use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use datafusion::prelude::SessionContext;
use datafusion_sql::unparser::dialect::{
    BigQueryDialect, DefaultDialect, DuckDBDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
};
use datafusion_sql::unparser::Unparser;
use vegafusion_runtime::data::codec::VegaFusionCodec;

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

    let sql = match dialect {
        "default" => {
            let d = DefaultDialect {};
            Unparser::new(&d).plan_to_sql(&plan)
        }
        "postgres" | "postgresql" => {
            let d = PostgreSqlDialect {};
            Unparser::new(&d).plan_to_sql(&plan)
        }
        "mysql" => {
            let d = MySqlDialect {};
            Unparser::new(&d).plan_to_sql(&plan)
        }
        "sqlite" => {
            let d = SqliteDialect {};
            Unparser::new(&d).plan_to_sql(&plan)
        }
        "duckdb" => {
            let d = DuckDBDialect::new();
            Unparser::new(&d).plan_to_sql(&plan)
        }
        "bigquery" => {
            let d = BigQueryDialect {};
            Unparser::new(&d).plan_to_sql(&plan)
        }
        _ => {
            return Err(PyValueError::new_err(format!(
                "Unknown dialect '{}'. Supported: default, postgres, mysql, sqlite, duckdb, bigquery",
                dialect
            )));
        }
    }
    .map_err(|e| PyValueError::new_err(format!("Failed to unparse plan to SQL: {e}")))?;

    Ok(sql.to_string())
}
