use crate::error::Result;
use crate::proto::gen::transforms::{MarkEncoding, MarkEncodingChannel};
use crate::spec::mark::MarkEncodingOrList;
use crate::spec::transform::mark_encoding::mark_encoding_input_vars;
use crate::spec::transform::mark_encoding::MarkEncodingTransformSpec;
use crate::transform::TransformDependencies;
use serde_json;

use crate::task_graph::task::InputVariable;

impl MarkEncoding {
    pub fn try_new(spec: &MarkEncodingTransformSpec) -> Result<Self> {
        let channels = spec
            .channels
            .iter()
            .map(|channel| {
                Ok(MarkEncodingChannel {
                    channel: channel.channel.clone(),
                    r#as: channel.as_.clone(),
                    encoding_json: serde_json::to_string(&channel.encoding)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            encode_set: spec.encode_set.clone(),
            channels,
        })
    }
}

impl TransformDependencies for MarkEncoding {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut vars = Vec::new();
        for channel in &self.channels {
            if let Ok(encoding) = serde_json::from_str::<MarkEncodingOrList>(&channel.encoding_json)
            {
                if let Ok(channel_vars) = mark_encoding_input_vars(&encoding) {
                    vars.extend(channel_vars);
                }
            }
        }
        vars
    }
}
