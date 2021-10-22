use crate::task_graph::scope::TaskScope;
use crate::spec::chart::ChartVisitor;
use crate::spec::mark::MarkSpec;
use crate::spec::data::DataSpec;
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::tasks::{Variable, Task, DataValuesTask, DataUrlTask, DataSourceTask};
use crate::spec::signal::SignalSpec;
use crate::spec::scale::ScaleSpec;
use serde_json::Value;
use crate::data::scalar::{ScalarValue, ScalarValueHelpers};
use crate::task_graph::task_value::TaskValue;
use crate::data::table::VegaFusionTable;
use crate::proto::gen::transforms::TransformPipeline;
use std::convert::TryFrom;
use crate::spec::values::StringOrSignalSpec;
use crate::proto::gen::tasks::data_url_task::Url;
use crate::expression::parser::parse;


#[derive(Clone, Debug, Default)]
pub struct BuildTaskScopeVisitor {
    pub task_scope: TaskScope,
}

impl BuildTaskScopeVisitor {
    pub fn new() -> Self {
        Self {
            task_scope: Default::default(),
        }
    }
}


impl ChartVisitor for BuildTaskScopeVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.data.insert(data.name.clone());
        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.signals.insert(signal.name.clone());
        Ok(())
    }

    fn visit_scale(&mut self, scale: &ScaleSpec, scope: &[u32]) -> Result<()> {
        let task_scope = self.task_scope.get_child_mut(scope)?;
        task_scope.scales.insert(scale.name.clone());
        Ok(())
    }

    fn visit_group_mark(&mut self, _mark: &MarkSpec, scope: &[u32]) -> Result<()> {
        // Initialize scope for this group level
        let parent_scope = self.task_scope.get_child_mut(&scope[0..scope.len() - 1])?;
        parent_scope.children.push(Default::default());
        Ok(())
    }
}


/// For a spec that is fully supported on the server, collect tasks
#[derive(Clone, Debug)]
pub struct MakeTasksVisitor {
    pub tasks: Vec<Task>
}

impl MakeTasksVisitor {
    pub fn new() -> Self {
        Self {
            tasks: Default::default()
        }
    }
}


impl ChartVisitor for MakeTasksVisitor {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        let data_var = Variable::new_data(&data.name);

        // Compute pipeline
        let pipeline = if data.transform.is_empty() {
            None
        } else {
            Some(TransformPipeline::try_from(data.transform.as_slice())?)
        };

        let task = if let Some(url) = &data.url {
            let proto_url = match url {
                StringOrSignalSpec::String(url) => {
                    Url::String(url.clone())
                }
                StringOrSignalSpec::Signal(expr) => {
                    let url_expr = parse(&expr.signal)?;
                    Url::Expr(url_expr)
                }
            };

            Task::new_data_url(
                data_var, scope, DataUrlTask {
                    batch_size: 8096,
                    format_type: None,
                    pipeline,
                    url: Some(proto_url)
                }
            )
        } else if let Some(source) = &data.source {
            Task::new_data_source(data_var, scope, DataSourceTask {
                source: source.clone(),
                pipeline
            })

        } else {
            let values_table = match data.values.as_ref() {
                Some(values) => {
                    VegaFusionTable::from_json(values, 1024)?
                },
                None => {
                    // Treat as empty values array
                    VegaFusionTable::from_json(
                        &Value::Array(Vec::new()), 1
                    )?
                }
            };

            if pipeline.is_none() {
                // If no transforms, treat as regular TaskValue task
                Task::new_value(
                    data_var, scope, TaskValue::Table(values_table)
                )
            } else {
                // Otherwise, create data values task (which supports transforms)
                Task::new_data_values(data_var, scope, DataValuesTask {
                    values: values_table.to_ipc_bytes()?,
                    pipeline
                })
            }
        };
        self.tasks.push(task);
        Ok(())
    }

    fn visit_signal(&mut self, signal: &SignalSpec, scope: &[u32]) -> Result<()> {
        let signal_var = Variable::new_signal(&signal.name);
        let value = match &signal.value {
            Some(value) => {
                TaskValue::Scalar(ScalarValue::from_json(&value)?)
            }
            None => {
                return Err(VegaFusionError::internal("Signal must have initial value"))
            }
        };
        let task = Task::new_value(signal_var, scope, value);
        self.tasks.push(task);
        Ok(())
    }

    fn visit_scale(&mut self, _scale: &ScaleSpec, _scope: &[u32]) -> Result<()> {
        unimplemented!("Scale tasks not yet supported")
    }
}
