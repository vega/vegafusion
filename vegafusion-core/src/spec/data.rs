use crate::error::Result;
use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::Variable;
use crate::spec::chart::ChartSpec;
use crate::spec::transform::TransformSpec;
use crate::spec::values::StringOrSignalSpec;
use crate::task_graph::scope::TaskScope;
use itertools::sorted;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use vegafusion_common::data::table::VegaFusionTable;
use vegafusion_common::error::VegaFusionError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSpec {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<StringOrSignalSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<DataFormatSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Value>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transform: Vec<TransformSpec>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub on: Option<Value>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl DataSpec {
    pub fn output_signals(&self) -> Vec<String> {
        let mut signals: HashSet<String> = Default::default();

        for tx in &self.transform {
            signals.extend(tx.output_signals())
        }

        sorted(signals).collect()
    }

    pub fn supported(
        &self,
        planner_config: &PlannerConfig,
        task_scope: &TaskScope,
        scope: &[u32],
    ) -> DependencyNodeSupported {
        if let Some(Some(format_type)) = self.format.as_ref().map(|fmt| fmt.type_.clone()) {
            if !matches!(format_type.as_str(), "csv" | "tsv" | "arrow" | "json") {
                // We don't know how to read the data, so full node is unsupported
                return DependencyNodeSupported::Unsupported;
            }
        }

        // Check if inline values array is supported
        if let Some(values) = &self.values {
            if !planner_config.extract_inline_data {
                return DependencyNodeSupported::Unsupported;
            }
            if !matches!(values, Value::Array(_)) {
                return DependencyNodeSupported::Unsupported;
            }
            if VegaFusionTable::from_json(values).is_err() {
                // Failed to read inline JSON as arrow, so unsupported
                return DependencyNodeSupported::Unsupported;
            }
        }

        let all_supported = self
            .transform
            .iter()
            .all(|tx| tx.supported_and_allowed(planner_config, task_scope, scope));
        if all_supported {
            DependencyNodeSupported::Supported
        } else if self.url.is_some() {
            DependencyNodeSupported::PartiallySupported
        } else {
            match self.transform.get(0) {
                Some(tx) if tx.supported_and_allowed(planner_config, task_scope, scope) => {
                    DependencyNodeSupported::PartiallySupported
                }
                _ => DependencyNodeSupported::Unsupported,
            }
        }
    }

    pub fn is_selection_store(&self) -> bool {
        self.name.ends_with("_store")
            && self.url.is_none()
            && self.source.is_none()
            && self.transform.is_empty()
    }

    pub fn local_datetime_columns_produced(
        &self,
        chart_spec: &ChartSpec,
        task_scope: &TaskScope,
        usage_scope: &[u32],
    ) -> Result<Vec<String>> {
        // Initialize output_local_datetime_columns
        let mut output_local_datetime_columns = if let Some(source) = &self.source {
            // We have a parent dataset, so init output_local_datetime_columns to be those
            // local datetime columns produced by the parent
            let source_var = Variable::new_data(source);
            let resolved = task_scope.resolve_scope(&source_var, usage_scope)?;
            let source_data = chart_spec.get_nested_data(resolved.scope.as_slice(), source)?;
            source_data.local_datetime_columns_produced(
                chart_spec,
                task_scope,
                resolved.scope.as_slice(),
            )?
        } else {
            // No parent dataset, so input local datetime columns is empty
            Default::default()
        };

        // Add any fields that are parsed as local datetimes
        if let Some(DataFormatParseSpec::Object(parse)) =
            self.format.as_ref().and_then(|format| format.parse.clone())
        {
            for (field, format) in parse {
                if format == "date" {
                    output_local_datetime_columns.push(field.clone())
                }
            }
        }

        // Propagate output_local_datetime_columns through transforms
        for tx in &self.transform {
            output_local_datetime_columns =
                tx.local_datetime_columns_produced(output_local_datetime_columns.as_slice())
        }

        Ok(output_local_datetime_columns)
    }

    /// Fuse this dataset into a child dataset. This mutates the child to include this dataset's
    /// source data and transforms. The name of the child is preserved.
    pub fn fuse_into(&self, child: &mut DataSpec) -> Result<()> {
        if Some(&self.name) != child.source.as_ref() {
            return Err(VegaFusionError::internal(format!(
                "Incompatible fuse dataset names {:?} and {:?}",
                self.name, child.source
            )));
        }
        if self.on.is_some() {
            return Err(VegaFusionError::internal(
                "Cannot fuse dataset with \"on\" trigger",
            ));
        }

        // Copy over source dataset info
        child.source = self.source.clone();
        child.url = self.url.clone();
        child.format = self.format.clone();
        child.values = self.values.clone();

        // Prepend this dataset's transforms
        let mut new_transforms = self.transform.clone();
        new_transforms.extend(child.transform.clone());
        child.transform = new_transforms;
        Ok(())
    }

    pub fn has_aggregate(&self) -> bool {
        self.transform
            .iter()
            .any(|tx| matches!(tx, &TransformSpec::Aggregate(_)))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
pub enum DependencyNodeSupported {
    Supported,
    PartiallySupported,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataFormatSpec {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub parse: Option<DataFormatParseSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataFormatParseSpec {
    Auto(String),
    Object(HashMap<String, String>),
}
