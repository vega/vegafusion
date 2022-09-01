/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use std::collections::HashSet;

use std::sync::Arc;
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::transforms::Project;

use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use datafusion_expr::col;
use vegafusion_core::expression::escape::unescape_field;
use vegafusion_core::task_graph::task_value::TaskValue;

#[async_trait]
impl TransformTrait for Project {
    async fn eval(
        &self,
        dataframe: Arc<SqlDataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<SqlDataFrame>, Vec<TaskValue>)> {
        // Collect all dataframe fields into a HashSet for fast membership test
        let all_fields: HashSet<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        // Keep all of the project columns that are present in the dataframe.
        // Skip projection fields that are not found
        let select_fields: Vec<_> = self
            .fields
            .iter()
            .filter_map(|field| {
                let field = unescape_field(field);
                if all_fields.contains(&field) {
                    Some(field)
                } else {
                    None
                }
            })
            .collect();

        let select_col_exprs: Vec<_> = select_fields.iter().map(|f| col(f)).collect();
        let result = dataframe.select(select_col_exprs)?;
        Ok((result, Default::default()))
    }
}
