/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::task_graph::timezone::RuntimeTzConfig;
use datafusion::logical_plan::{DFSchema, Expr};
use vegafusion_core::data::table::VegaFusionTable;
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
