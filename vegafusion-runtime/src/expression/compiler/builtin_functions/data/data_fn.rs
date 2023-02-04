use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion_expr::Expr;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::datafusion_common::DFSchema;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::expression::Expression;

pub fn data_fn(
    table: &VegaFusionTable,
    _args: &[Expression],
    _schema: &DFSchema,
    _tz_config: &RuntimeTzConfig,
) -> Result<Expr> {
    Ok(Expr::Literal(table.to_scalar_value()?))
}
