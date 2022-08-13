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

use itertools::Itertools;
use std::collections::HashMap;

use std::sync::Arc;
use vegafusion_core::error::{Result, VegaFusionError};

use crate::sql::dataframe::SqlDataFrame;
use async_trait::async_trait;
use vegafusion_core::data::table::VegaFusionTable;
use vegafusion_core::proto::gen::tasks::{Variable, VariableNamespace};
use vegafusion_core::proto::gen::transforms::TransformPipeline;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_core::transform::TransformDependencies;
use crate::data::table::VegaFusionTableUtils;

#[async_trait]
pub trait TransformPipelineUtils {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)>;

    async fn eval_sql(
        &self,
        dataframe: Arc<SqlDataFrame>,
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)>;
}

#[async_trait]
impl TransformPipelineUtils for TransformPipeline {
    async fn eval(
        &self,
        dataframe: Arc<DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(VegaFusionTable, Vec<TaskValue>)> {
        let mut result_df = dataframe;
        let mut result_outputs: HashMap<Variable, TaskValue> = Default::default();
        let mut config = config.clone();

        for tx in &self.transforms {
            let tx_result = tx.eval(result_df, &config).await?;

            // Update dataframe
            result_df = tx_result.0;

            for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
                result_outputs.insert(var.clone(), val.clone());

                // Also add output signals to config scope so they are available to the following
                // transforms
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
            }
        }

        // Sort result signal value by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = result_outputs
            .into_iter()
            .sorted_by_key(|(k, _v)| k.clone())
            .unzip();

        let table = VegaFusionTable::from_dataframe(result_df).await?;
        Ok((table, signals_values))
    }

    async fn eval_sql(&self, sql_df: Arc<SqlDataFrame>, config: &CompilationConfig) -> Result<(VegaFusionTable, Vec<TaskValue>)> {
        let mut result_sql_df = sql_df;
        let mut result_outputs: HashMap<Variable, TaskValue> = Default::default();
        let mut config = config.clone();

        // Helper function to add variable value to config
        let add_output_var_to_config = |config: &mut CompilationConfig, var: &Variable, val: TaskValue| -> Result<()> {
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

        let mut next_tx_ind = 0;
        for (tx_ind, tx) in self.transforms.iter().enumerate() {
            // Append transform and update result df
            let tx_result = match tx.eval_sql(result_sql_df.clone(), &config).await {
                Ok(tx_result) => tx_result,
                Err(VegaFusionError::SqlNotSupported(_msg, ..)) => {
                    // Transform not support using SQL, break out of for loop and
                    // handle this transform and then remaining with DataFrames
                    println!("{}", _msg);
                    break
                }
                Err(err) => return Err(err)
            };

            result_sql_df = tx_result.0;

            // Collect output variables
            for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
                result_outputs.insert(var.clone(), val.clone());

                // Also add output signals to config scope so they are available to the following
                // transforms
                add_output_var_to_config(&mut config, var, val)?;
            }
            next_tx_ind = tx_ind + 1;
        }

        let table = if next_tx_ind == self.transforms.len() {
            // All transforms support SQL
            result_sql_df.collect().await?
        } else {
            // Some transforms do not support sql, perform these with DataFrames
            let mut result_df = result_sql_df.collect().await?.to_dataframe()?;
            for tx in self.transforms.iter().skip(next_tx_ind) {
                let tx_result = tx.eval(result_df, &config).await?;

                // Update dataframe
                result_df = tx_result.0;

                // Collect output variables
                for (var, val) in tx.output_vars().iter().zip(tx_result.1) {
                    result_outputs.insert(var.clone(), val.clone());

                    // Also add output signals to config scope so they are available to the following
                    // transforms
                    add_output_var_to_config(&mut config, var, val)?;
                }
            }
            VegaFusionTable::from_dataframe(result_df).await?
        };

        // Sort result signal value by signal name
        let (_, signals_values): (Vec<_>, Vec<_>) = result_outputs
            .into_iter()
            .sorted_by_key(|(k, _v)| k.clone())
            .unzip();


        Ok((table, signals_values))
    }
}
