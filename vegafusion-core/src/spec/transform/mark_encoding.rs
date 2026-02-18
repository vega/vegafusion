use crate::error::Result;
use crate::expression::column_usage::{
    ColumnUsage, DatasetsColumnUsage, GetDatasetsColumnUsage, VlSelectionFields,
};
use crate::expression::parser::parse;
use crate::expression::visitors::ExpressionVisitor;
use crate::proto::gen::tasks::Variable;
use crate::spec::mark::{EncodingOffset, MarkEncodingField, MarkEncodingOrList, MarkEncodingSpec};
use crate::spec::transform::{TransformColumns, TransformSpecTrait};
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

fn default_encode_set() -> String {
    "update".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkEncodingTransformSpec {
    #[serde(default = "default_encode_set")]
    pub encode_set: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub channels: Vec<MarkEncodingChannelSpec>,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkEncodingChannelSpec {
    pub channel: String,

    #[serde(rename = "as")]
    pub as_: String,

    pub encoding: MarkEncodingOrList,

    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TransformSpecTrait for MarkEncodingTransformSpec {
    fn supported(&self) -> bool {
        self.extra.is_empty()
            && self
                .channels
                .iter()
                .all(mark_encoding_channel_spec_supported)
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        let mut vars: HashSet<InputVariable> = Default::default();
        for channel in &self.channels {
            vars.extend(mark_encoding_channel_input_vars(channel)?);
        }
        Ok(vars.into_iter().sorted().collect())
    }

    fn transform_columns(
        &self,
        datum_var: &Option<ScopedVariable>,
        usage_scope: &[u32],
        task_scope: &TaskScope,
        vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        let Some(datum_var) = datum_var else {
            return TransformColumns::Unknown;
        };

        let mut usage = DatasetsColumnUsage::empty();
        for channel in &self.channels {
            for encoding in channel.encoding.to_vec() {
                usage = usage.union(&encoding.datasets_column_usage(
                    &Some(datum_var.clone()),
                    usage_scope,
                    task_scope,
                    vl_selection_fields,
                ));
            }
        }

        let mut produced = ColumnUsage::empty();
        for channel in &self.channels {
            produced = produced.with_column(&channel.as_);
        }

        TransformColumns::PassThrough { usage, produced }
    }

    fn local_datetime_columns_produced(
        &self,
        input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        // mark_encoding is pass-through and only appends derived channel columns.
        Vec::from(input_local_datetime_columns)
    }
}

pub fn mark_encoding_channel_spec_supported(channel: &MarkEncodingChannelSpec) -> bool {
    channel.extra.is_empty()
        // Fail closed for text channels in this phase. Vega text rendering applies additional
        // formatting/coercion behavior that isn't fully mirrored yet by server-side precompute.
        && !channel.channel.eq_ignore_ascii_case("text")
        // Tooltips are non-visual for static rendering and can rely on full-datum semantics
        // (for example `signal: "datum"`) that this phase of mark_encoding does not mirror.
        && !channel.channel.eq_ignore_ascii_case("tooltip")
        && mark_encoding_supported(&channel.encoding)
}

pub fn mark_encoding_supported(encoding: &MarkEncodingOrList) -> bool {
    encoding.to_vec().iter().all(mark_encoding_rule_supported)
}

pub fn mark_encoding_channel_input_vars(
    channel: &MarkEncodingChannelSpec,
) -> Result<Vec<InputVariable>> {
    mark_encoding_input_vars(&channel.encoding)
}

pub fn mark_encoding_input_vars(encoding: &MarkEncodingOrList) -> Result<Vec<InputVariable>> {
    let mut vars: HashSet<InputVariable> = Default::default();
    for rule in encoding.to_vec() {
        collect_rule_input_vars(&rule, &mut vars)?;
    }
    Ok(vars.into_iter().sorted().collect())
}

fn mark_encoding_rule_supported(rule: &MarkEncodingSpec) -> bool {
    if let Some(band) = &rule.band {
        if band.as_f64().is_none_or(|v| !v.is_finite()) {
            return false;
        }
        if rule.scale.is_none() {
            return false;
        }
    }

    // Only allow mult / round as extra keys
    if !rule
        .extra
        .keys()
        .all(|k| matches!(k.as_str(), "mult" | "round"))
    {
        return false;
    }

    if let Some(signal) = &rule.signal {
        let Ok(parsed) = parse(signal) else {
            return false;
        };
        if !parsed.is_supported() || expr_contains_format_call(&parsed) {
            return false;
        }
    }

    if let Some(test) = &rule.test {
        let Ok(parsed) = parse(test) else {
            return false;
        };
        if !parsed.is_supported() || expr_contains_format_call(&parsed) {
            return false;
        }
    }

    if let Some(field) = &rule.field {
        match field {
            MarkEncodingField::Field(_) => {}
            MarkEncodingField::Object(field_obj) => {
                if field_obj.datum.is_none()
                    || field_obj.group.is_some()
                    || field_obj.parent.is_some()
                    || field_obj.signal.is_some()
                    || !field_obj.extra.is_empty()
                {
                    return false;
                }
            }
        }
    }

    if let Some(mult) = rule.extra.get("mult") {
        if !(mult.is_number() || is_signal_object(mult)) {
            return false;
        }
        if let Some(signal) = mult
            .as_object()
            .and_then(|obj| obj.get("signal"))
            .and_then(|v| v.as_str())
        {
            let Ok(parsed) = parse(signal) else {
                return false;
            };
            if !parsed.is_supported() || expr_contains_format_call(&parsed) {
                return false;
            }
        }
    }

    if let Some(round) = rule.extra.get("round") {
        if !(round.is_boolean() || is_signal_object(round)) {
            return false;
        }
        if let Some(signal) = round
            .as_object()
            .and_then(|obj| obj.get("signal"))
            .and_then(|v| v.as_str())
        {
            let Ok(parsed) = parse(signal) else {
                return false;
            };
            if !parsed.is_supported() || expr_contains_format_call(&parsed) {
                return false;
            }
        }
    }

    if let Some(offset) = &rule.offset {
        match offset {
            EncodingOffset::Encoding(offset_expr) => {
                if !mark_encoding_rule_supported(offset_expr) {
                    return false;
                }
            }
            EncodingOffset::Value(v) => {
                if !(v.is_number() || is_signal_object(v)) {
                    return false;
                }
                if let Some(signal) = v
                    .as_object()
                    .and_then(|obj| obj.get("signal"))
                    .and_then(|v| v.as_str())
                {
                    let Ok(parsed) = parse(signal) else {
                        return false;
                    };
                    if !parsed.is_supported() || expr_contains_format_call(&parsed) {
                        return false;
                    }
                }
            }
        }
    }

    true
}

fn collect_rule_input_vars(
    rule: &MarkEncodingSpec,
    vars: &mut HashSet<InputVariable>,
) -> Result<()> {
    if let Some(scale) = &rule.scale {
        vars.insert(InputVariable {
            var: Variable::new_scale(scale),
            propagate: true,
        });
    }

    if let Some(signal) = &rule.signal {
        vars.extend(parse(signal)?.input_vars());
    }

    if let Some(test) = &rule.test {
        vars.extend(parse(test)?.input_vars());
    }

    if let Some(mult) = rule
        .extra
        .get("mult")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("signal"))
        .and_then(|v| v.as_str())
    {
        vars.extend(parse(mult)?.input_vars());
    }

    if let Some(round) = rule
        .extra
        .get("round")
        .and_then(|v| v.as_object())
        .and_then(|obj| obj.get("signal"))
        .and_then(|v| v.as_str())
    {
        vars.extend(parse(round)?.input_vars());
    }

    if let Some(offset) = &rule.offset {
        match offset {
            EncodingOffset::Encoding(offset_expr) => {
                collect_rule_input_vars(offset_expr, vars)?;
            }
            EncodingOffset::Value(v) => {
                if let Some(signal) = v
                    .as_object()
                    .and_then(|obj| obj.get("signal"))
                    .and_then(|v| v.as_str())
                {
                    vars.extend(parse(signal)?.input_vars());
                }
            }
        }
    }

    Ok(())
}

fn is_signal_object(value: &Value) -> bool {
    value
        .as_object()
        .map(|obj| obj.len() == 1 && obj.contains_key("signal"))
        .unwrap_or(false)
}

fn expr_contains_format_call(expr: &crate::proto::gen::expression::Expression) -> bool {
    struct ContainsFormatCall {
        found: bool,
    }

    impl ExpressionVisitor for ContainsFormatCall {
        fn visit_called_identifier(
            &mut self,
            node: &crate::proto::gen::expression::Identifier,
            _args: &[crate::proto::gen::expression::Expression],
        ) {
            if node.name == "format" {
                self.found = true;
            }
        }
    }

    let mut visitor = ContainsFormatCall { found: false };
    expr.walk(&mut visitor);
    visitor.found
}

#[cfg(test)]
mod tests {
    use super::{
        mark_encoding_channel_input_vars, mark_encoding_channel_spec_supported,
        MarkEncodingChannelSpec,
    };
    use crate::proto::gen::tasks::Variable;
    use crate::spec::mark::MarkEncodingOrList;
    use serde_json::json;

    #[test]
    fn test_mark_encoding_channel_support_fail_closed() {
        let supported: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "x",
            "as": "x_px",
            "encoding": {"field": "v", "scale": "x", "offset": 2}
        }))
        .unwrap();
        assert!(mark_encoding_channel_spec_supported(&supported));

        let supported_band: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "x",
            "as": "x_center",
            "encoding": {"field": "v", "scale": "x", "band": 0.5}
        }))
        .unwrap();
        assert!(mark_encoding_channel_spec_supported(&supported_band));

        let unsupported: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "x",
            "as": "x_px",
            "encoding": {"field": {"group": "foo"}}
        }))
        .unwrap();
        assert!(!mark_encoding_channel_spec_supported(&unsupported));

        let unsupported_band_without_scale: MarkEncodingChannelSpec =
            serde_json::from_value(json!({
                "channel": "width",
                "as": "w",
                "encoding": {"band": 1}
            }))
            .unwrap();
        assert!(!mark_encoding_channel_spec_supported(
            &unsupported_band_without_scale
        ));

        let unsupported_format: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "text",
            "as": "text_val",
            "encoding": {"signal": "format(datum.v, '')"}
        }))
        .unwrap();
        assert!(!mark_encoding_channel_spec_supported(&unsupported_format));

        let unsupported_text_channel: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "text",
            "as": "text_val",
            "encoding": {"signal": "datum.v"}
        }))
        .unwrap();
        assert!(!mark_encoding_channel_spec_supported(
            &unsupported_text_channel
        ));

        let unsupported_tooltip_channel: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "tooltip",
            "as": "tooltip_val",
            "encoding": {"signal": "datum"}
        }))
        .unwrap();
        assert!(!mark_encoding_channel_spec_supported(
            &unsupported_tooltip_channel
        ));
    }

    #[test]
    fn test_mark_encoding_channel_input_vars_include_scale_and_signals() {
        let channel: MarkEncodingChannelSpec = serde_json::from_value(json!({
            "channel": "x",
            "as": "x_px",
            "encoding": [
                {
                    "test": "test_sig > 0",
                    "field": "v",
                    "scale": "x",
                    "mult": {"signal": "mult_sig"},
                    "offset": {"signal": "offset_sig"}
                },
                {"value": 0}
            ]
        }))
        .unwrap();

        let vars = mark_encoding_channel_input_vars(&channel).unwrap();
        let vars: std::collections::HashSet<_> = vars.into_iter().map(|v| v.var).collect();

        assert!(vars.contains(&Variable::new_scale("x")));
        assert!(vars.contains(&Variable::new_signal("test_sig")));
        assert!(vars.contains(&Variable::new_signal("mult_sig")));
        assert!(vars.contains(&Variable::new_signal("offset_sig")));
    }

    #[test]
    fn test_mark_encoding_transform_json_round_trip() {
        let tx = json!({
            "type": "mark_encoding",
            "encode_set": "update",
            "channels": [
                {
                    "channel": "x",
                    "as": "x_px",
                    "encoding": {"field": "v", "scale": "x"}
                }
            ]
        });

        let parsed: crate::spec::transform::TransformSpec = serde_json::from_value(tx).unwrap();
        match parsed {
            crate::spec::transform::TransformSpec::MarkEncoding(mark_encoding) => {
                assert_eq!(mark_encoding.encode_set, "update");
                assert_eq!(mark_encoding.channels.len(), 1);
                assert!(matches!(
                    mark_encoding.channels[0].encoding,
                    MarkEncodingOrList::Scalar(_)
                ));
            }
            _ => panic!("Expected mark_encoding transform"),
        }
    }
}
