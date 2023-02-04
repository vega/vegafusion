use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::error::Result;
use vegafusion_core::proto::gen::transforms::Project;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Project {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        // Collect all dataframe fields into a HashSet for fast membership test
        let all_fields: HashSet<_> = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        // Keep all of the project columns that are present in the dataframe.
        // Skip projection fields that are not found
        let mut select_fields: Vec<_> = self
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

        // Always keep ordering column
        select_fields.insert(0, ORDER_COL.to_string());

        let select_col_exprs: Vec<_> = select_fields.iter().map(|f| flat_col(f)).collect();
        let result = dataframe.select(select_col_exprs).await?;
        Ok((result, Default::default()))
    }
}
