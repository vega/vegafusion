use crate::task_graph::scope::TaskScope;
use crate::spec::chart::ChartVisitor;
use crate::spec::mark::MarkSpec;
use crate::spec::data::DataSpec;
use crate::error::{Result, VegaFusionError};
use crate::proto::gen::tasks::{Variable, Task};
use crate::spec::signal::SignalSpec;
use crate::spec::scale::ScaleSpec;
use serde_json::Value;
use crate::data::scalar::{ScalarValue, ScalarValueHelpers};
use crate::task_graph::task_value::TaskValue;
use crate::data::table::VegaFusionTable;
use crate::proto::gen::transforms::TransformPipeline;
use std::convert::TryFrom;


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
pub struct MakeTasksVisitor<'a> {
    pub task_scope: &'a TaskScope,
    pub tasks: Vec<Task>
}

impl <'a> MakeTasksVisitor<'a> {
    pub fn new(task_scope: &'a TaskScope) -> Self {
        Self {
            task_scope,
            tasks: Default::default()
        }
    }
}


impl <'a> ChartVisitor for MakeTasksVisitor<'a> {
    fn visit_data(&mut self, data: &DataSpec, scope: &[u32]) -> Result<()> {
        todo!()
        // let data_var = Variable::new_data(&signal.name);
        // let task_scope = self.task_scope.get_child_mut(scope)?;
        //
        // if let Some(values) = &data.values {
        //     let table = VegaFusionTable::from_json(values, 1024)?;
        //     let value = TaskValue::Table(table);
        //
        //     if data.transform.is_empty() {
        //         // Add value task as data_var
        //         let task = Task::new_value(data_var, scope, value);
        //         self.tasks.push(task);
        //     } else {
        //         // Add _raw suffix to data set's name to store raw values
        //         let mut values_name = data.name.clone();
        //         values_name.push_str("_raw");
        //         let values_var = Variable::new_data(&values_name);
        //
        //         // Add raw values as Value task
        //         let task = Task::new_value(values_var, scope, value);
        //         self.tasks.push(task);
        //
        //         // Add to _raw dataset declaration to scope
        //         task_scope.data.insert(values_name);
        //
        //         // Build transform task
        //         let pipeline = TransformPipeline::try_from(&data.transform)?;
        //         let transform_task = TransformsTask {
        //             source: values_name.clone(),
        //             pipeline: Some(pipeline)
        //         };
        //         let task = Task::new_transforms(data_var, scope, transform_task);
        //     }
        // } else if let Some(url) = &data.url {
        //     // Add value task to hold URL string
        //     let mut url_var_name = data.name.clone();
        //     url_var_name.push_str("_url");
        //     let url_var = Variable::new_signal(&url_var_name);
        //     let url_value = TaskValue::Scalar(ScalarValue::from(url));
        //
        //     // Add value task
        //     let task = Task::new_value(url_var.clone(), scope, url_value);
        //     self.tasks.push(task);
        //
        //     // Add _url signal to signal scope
        //     task_scope.signals.insert(url_var_name);
        //
        //     // Make task to load data from url
        //     let url_task = ScanUrlTask {
        //         url: Some(url_var),
        //         batch_size: 8096,
        //         format_type: None
        //     };
        //
        //     if data.transform.is_empty() {
        //         let task = Task::new_scan_url(data_var, scope, url_task);
        //         self.tasks.push(task);
        //     } else {
        //         return Err(VegaFusionError::internal("url data tasks may not yet have transforms"))
        //     }
        // } else if let Some(source) = &data.source {
        //     // Make transform task
        //     let pipeline = TransformPipeline::try_from(&data.transform)?;
        //     let transform_task = TransformsTask {
        //         source: source.clone(),
        //         pipeline: Some(pipeline)
        //     };
        //     let task = Task::new_transforms(data_var, scope, transform_task);
        //
        //
        // }
        //
        //
        //
        // let task = TransformsTask {
        //     source: data.source.unwrap_or("".to_string()),
        //     pipeline: None
        // };
        // Task::new_transforms(data_var, scope, )
        //
        // let task_scope = self.task_scope.get_child_mut(scope)?;
        // task_scope.data.insert(data.name.clone());
        // Ok(())
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

