use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;

use itertools::Itertools;
use std::collections::HashMap;

use async_trait::async_trait;
use datafusion_expr::{expr, lit, Expr};
use std::sync::Arc;
use vegafusion_common::column::flat_col;
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::data::ORDER_COL;
use vegafusion_common::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::tasks::{Variable, VariableNamespace};
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::TransformDependencies;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
pub trait TransformPipelineUtils {
    async fn eval_sql(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)>;
}

#[async_trait]
impl TransformPipelineUtils for TransformPipeline {
    async fn eval_sql(
        &self,
        sql_df: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)> {
        let mut result_sql_df = sql_df;
        let mut result_outputs: HashMap<Variable, TaskValue> = Default::default();
        let mut config = config.clone();

        if result_sql_df.schema().column_with_name(ORDER_COL).is_none() {
            return Err(VegaFusionError::internal(format!(
                "DataFrame input to eval_sql does not have the expected {ORDER_COL} ordering column"
            )));
        }

        // Helper function to add variable value to config
        let add_output_var_to_config =
            |config: &mut CompilationConfig, var: &Variable, val: TaskValue| -> Result<()> {
                match var.ns() {
                    VariableNamespace::Signal => {
                        config
                            .signal_scope
                            .insert(var.name.clone(), val.as_scalar()?.clone());
                    }
                    VariableNamespace::Data => {
                        config
                            .data_scope
                            .insert(var.name.clone(), val.as_table()?.clone());
                    }
                    VariableNamespace::Scale => {
                        unimplemented!()
                    }
                }
                Ok(())
            };

        for tx in self.transforms.iter() {
            // Append transform and update result df
            let tx_result = tx.eval(result_sql_df.clone(), &config).await?;

            result_sql_df = tx_result.0;

            if result_sql_df.schema().column_with_name(ORDER_COL).is_none() {
                return Err(VegaFusionError::internal(
                    format!("DataFrame output of transform does not have the expected {ORDER_COL} ordering column: {tx:?}")
                ));
            }

            // Collect output variables
            for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
                result_outputs.insert(var.clone(), val.clone());

                // Also add output signals to config scope so they are available to the following
                // transforms
                add_output_var_to_config(&mut config, var, val)?;
            }
        }

        // Sort by ordering column at the end
        result_sql_df = result_sql_df
            .sort(
                vec![Expr::Sort(expr::Sort {
                    expr: Box::new(flat_col(ORDER_COL)),
                    asc: true,
                    nulls_first: false,
                })],
                None,
            )
            .await?;

        // Remove ordering column
        result_sql_df = remove_order_col(result_sql_df).await?;

        let table = result_sql_df.collect().await?;

        // Sort result signal value by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = result_outputs
            .into_iter()
            .sorted_by_key(|(k, _v)| k.clone())
            .unzip();

        Ok((table, signals_values))
    }
}

pub async fn remove_order_col(result_sql_df: Arc<dyn DataFrame>) -> Result<Arc<dyn DataFrame>> {
    let mut selection = result_sql_df
        .schema()
        .fields
        .iter()
        .filter_map(|field| {
            if field.name() == ORDER_COL {
                None
            } else {
                Some(flat_col(field.name()))
            }
        })
        .collect::<Vec<_>>();

    // Add arbitrary column so that SELECT succeeds
    if selection.is_empty() {
        selection.push(lit(0).alias("_empty"))
    }

    result_sql_df.select(selection).await
}
