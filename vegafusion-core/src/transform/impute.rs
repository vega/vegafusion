use crate::error::Result;
use crate::proto::gen::transforms::{Impute, ImputeMethod};
use crate::spec::transform::impute::{ImputeMethodSpec, ImputeTransformSpec};
use crate::transform::TransformDependencies;

impl Impute {
    pub fn try_new(spec: &ImputeTransformSpec) -> Result<Self> {
        // Extract method
        let method = match spec.method() {
            ImputeMethodSpec::Value => ImputeMethod::ImputeValue,
            ImputeMethodSpec::Mean => ImputeMethod::ImputeMean,
            ImputeMethodSpec::Median => ImputeMethod::ImputeMedian,
            ImputeMethodSpec::Max => ImputeMethod::ImputeMax,
            ImputeMethodSpec::Min => ImputeMethod::ImputeMin,
        };

        // Extract field
        let field = spec.field.field();

        // Extract key
        let key = spec.key.field();

        // Extract groupby
        let groupby: Vec<_> = spec
            .groupby
            .clone()
            .unwrap_or_default()
            .iter()
            .map(|field| field.field())
            .collect();

        // Extract Value
        let value_json = spec
            .value
            .as_ref()
            .map(|value| serde_json::to_string(value).unwrap());

        // keyvals not yet supported

        Ok(Impute {
            field,
            key,
            method: method as i32,
            groupby,
            value_json,
        })
    }
}

impl TransformDependencies for Impute {}
