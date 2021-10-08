use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::compiler::config::CompilationConfig;
use crate::spec::transform::collect::{CollectTransformSpec, SortOrder};
use crate::transform::base::TransformTrait;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{col, Expr};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct CollectTransform {
    pub fields: Vec<String>,
    pub order: Vec<SortOrder>,
}

impl CollectTransform {
    pub fn try_new(transform: &CollectTransformSpec) -> Result<Self> {
        let sort = &transform.sort;
        let fields = sort.field.to_vec();
        let order = match &sort.order {
            None => {
                vec![SortOrder::Ascending; fields.len()]
            }
            Some(order) => {
                let order = order.to_vec();
                if order.len() == fields.len() {
                    order
                } else {
                    return Err(VegaFusionError::specification(
                        "Length of field and order must match in collect transform",
                    ));
                }
            }
        };

        Ok(Self { fields, order })
    }
}

impl TransformTrait for CollectTransform {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        _config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        let sort_exprs: Vec<_> = self
            .fields
            .clone()
            .into_iter()
            .zip(&self.order)
            .map(|(field, order)| Expr::Sort {
                expr: Box::new(col(&field)),
                asc: matches!(order, SortOrder::Ascending),
                nulls_first: matches!(order, SortOrder::Ascending),
            })
            .collect();

        let result = dataframe
            .sort(sort_exprs)
            .with_context(|| "Collect transform failed".to_string())?;
        Ok((result, Default::default()))
    }
}
