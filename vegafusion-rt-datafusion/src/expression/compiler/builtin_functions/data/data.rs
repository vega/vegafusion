use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::proto::gen::expression::Expression;
use vegafusion_core::error::Result;
use datafusion::logical_plan::{DFSchema, Expr};

pub fn data_fn(
    table: &VegaFusionTable,
    _args: &[Expression],
    _schema: &DFSchema,
) -> Result<Expr> {
    Ok(Expr::Literal(table.to_scalar_value()?))
}
