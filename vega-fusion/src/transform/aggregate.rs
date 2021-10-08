use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::compiler::config::CompilationConfig;
use crate::spec::transform::aggregate::{AggregateOp, AggregateTransformSpec};
use crate::transform::base::TransformTrait;
use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{count_distinct, Expr};
use datafusion::prelude::{avg, col, count, lit, max, min, sum};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct AggregateTransform {
    pub groupby: Vec<String>,
    pub fields: Vec<Option<String>>,
    pub ops: Vec<AggregateOp>,
    pub aliases: Vec<Option<String>>,
}

impl AggregateTransform {
    pub fn new(transform: &AggregateTransformSpec) -> Self {
        let fields: Vec<_> = transform
            .fields
            .iter()
            .map(|f| f.as_ref().map(|f| f.field()))
            .collect();

        let groupby: Vec<_> = transform.groupby.iter().map(|f| f.field()).collect();

        // Initialize aliases with those potentially provided in field objects
        // (e.g. {"field": "foo", "as": "bar"}
        let mut aliases: Vec<_> = transform
            .fields
            .iter()
            .map(|f| f.as_ref().and_then(|f| f.as_()))
            .collect();

        // Overwrite aliases with those provided in the as_ prop of the transform
        for (i, as_) in transform.as_.clone().unwrap_or_default().iter().enumerate() {
            if as_.is_some() {
                aliases[i] = as_.clone();
            }
        }

        Self {
            groupby,
            fields,
            ops: transform.ops.clone(),
            aliases,
        }
    }
}

impl TransformTrait for AggregateTransform {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let mut agg_exprs = Vec::new();
        for (i, (field, op)) in self.fields.iter().zip(self.ops.iter()).enumerate() {
            let column = if matches!(op, AggregateOp::Count) {
                // In Vega, the provided column is always ignored if op is 'count'.
                lit(0)
            } else {
                match field.as_ref() {
                    Some(column) => col(column),
                    None => {
                        return Err(VegaFusionError::specification(&format!(
                            "Null field is not allowed for {:?} op",
                            op
                        )))
                    }
                }
            };

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
            let expr = if let Some(Some(alias)) = self.aliases.get(i) {
                expr.alias(alias)
            } else {
                expr.alias(&format!(
                    "{}_{}",
                    op.name(),
                    field.as_ref().unwrap_or(&String::from("null"))
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
