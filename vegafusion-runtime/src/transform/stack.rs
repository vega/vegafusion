use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion_expr::{expr, Expr};
use std::sync::Arc;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::Result;
use vegafusion_common::escape::unescape_field;
use vegafusion_core::proto::gen::transforms::{SortOrder, Stack, StackOffset};
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::{DataFrame, StackMode};

#[async_trait]
impl TransformTrait for Stack {
    async fn eval(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let start_field = self.alias_0.clone().expect("alias0 expected");
        let stop_field = self.alias_1.clone().expect("alias1 expected");

        let field = unescape_field(&self.field);
        let group_by: Vec<_> = self.groupby.iter().map(|f| unescape_field(f)).collect();

        // Build order by vector
        let mut order_by: Vec<_> = self
            .sort_fields
            .iter()
            .zip(&self.sort)
            .map(|(field, order)| {
                Expr::Sort(expr::Sort {
                    expr: Box::new(unescaped_col(field)),
                    asc: *order == SortOrder::Ascending as i32,
                    nulls_first: *order == SortOrder::Ascending as i32,
                })
            })
            .collect();

        // Order by input row ordering last
        order_by.push(Expr::Sort(expr::Sort {
            expr: Box::new(flat_col(ORDER_COL)),
            asc: true,
            nulls_first: true,
        }));

        let offset = StackOffset::try_from(self.offset).expect("Failed to convert stack offset");
        let mode = match offset {
            StackOffset::Zero => StackMode::Zero,
            StackOffset::Normalize => StackMode::Normalize,
            StackOffset::Center => StackMode::Center,
        };

        let result = dataframe
            .stack(
                &field,
                order_by,
                group_by.as_slice(),
                &start_field,
                &stop_field,
                mode,
            )
            .await?;
        Ok((result, Default::default()))
    }
}
