use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{avg, col, count, count_distinct, lit, max, min, sum, Expr};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;
use vegafusion_core::arrow::datatypes::DataType;
use vegafusion_core::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::proto::gen::transforms::{Aggregate, AggregateOp};
use vegafusion_core::transform::aggregate::op_name;
use async_trait::async_trait;
use vegafusion_core::task_graph::task_value::TaskValue;


#[async_trait]
impl TransformTrait for Aggregate {
    async fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let mut agg_exprs = Vec::new();
        for (i, (field, op)) in self.fields.iter().zip(self.ops.iter()).enumerate() {
            let column = if *op == AggregateOp::Count as i32 {
                // In Vega, the provided column is always ignored if op is 'count'.
                lit(0)
            } else {
                match field.as_str() {
                    "" => {
                        return Err(VegaFusionError::specification(&format!(
                            "Null field is not allowed for {:?} op",
                            op
                        )))
                    }
                    column => col(column),
                }
            };

            let op = AggregateOp::from_i32(*op).unwrap();

            let expr = match op {
                AggregateOp::Count => count(column),
                AggregateOp::Mean | AggregateOp::Average => avg(column),
                AggregateOp::Min => min(column),
                AggregateOp::Max => max(column),
                AggregateOp::Sum => sum(column),
                AggregateOp::Valid => {
                    let valid = Expr::Cast {
                        expr: Box::new(Expr::IsNotNull(Box::new(column))),
                        data_type: DataType::UInt64,
                    };
                    sum(valid)
                }
                AggregateOp::Missing => {
                    let missing = Expr::Cast {
                        expr: Box::new(Expr::IsNull(Box::new(column))),
                        data_type: DataType::UInt64,
                    };
                    sum(missing)
                }
                AggregateOp::Distinct => count_distinct(column),
                _ => {
                    return Err(VegaFusionError::specification(&format!(
                        "Unsupported aggregation op: {:?}",
                        op
                    )))
                }
            };

            // Apply alias
            let expr = if let Some(alias) = self.aliases.get(i).filter(|a| !a.is_empty()) {
                // Alias is a non-empty string
                expr.alias(alias)
            } else {
                expr.alias(&format!(
                    "{}_{}",
                    op_name(op),
                    (if field.is_empty() { "null" } else { field }).to_string(),
                ))
            };
            agg_exprs.push(expr)
        }

        let group_exprs: Vec<_> = self.groupby.iter().map(|c| col(c)).collect();

        let grouped_dataframe = dataframe
            .aggregate(group_exprs, agg_exprs)
            .with_context(|| "Failed to perform aggregate transform".to_string())?;

        Ok((grouped_dataframe, Vec::new()))
    }
}
