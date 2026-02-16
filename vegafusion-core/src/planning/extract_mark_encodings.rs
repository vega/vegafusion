use crate::error::Result;
use crate::planning::plan::PlannerConfig;
use crate::proto::gen::tasks::{Variable, VariableNamespace};
use crate::spec::chart::ChartSpec;
use crate::spec::data::DataSpec;
use crate::spec::mark::{MarkEncodingField, MarkEncodingOrList, MarkEncodingSpec, MarkSpec};
use crate::spec::scale::{ScaleSpec, ScaleTypeSpec};
use crate::spec::transform::mark_encoding::{
    mark_encoding_input_vars, mark_encoding_supported, MarkEncodingChannelSpec,
    MarkEncodingTransformSpec,
};
use crate::spec::transform::TransformSpec;
use crate::task_graph::scope::TaskScope;
use itertools::Itertools;

pub fn extract_mark_encodings(
    client_spec: &mut ChartSpec,
    server_spec: &mut ChartSpec,
    task_scope: &mut TaskScope,
    config: &PlannerConfig,
) -> Result<()> {
    if !config.precompute_mark_encodings {
        return Ok(());
    }

    process_marks(
        &mut client_spec.marks,
        &[],
        false,
        server_spec,
        task_scope,
        config,
    )
}

fn process_marks(
    marks: &mut [MarkSpec],
    scope: &[u32],
    in_facet_group: bool,
    server_spec: &mut ChartSpec,
    task_scope: &mut TaskScope,
    config: &PlannerConfig,
) -> Result<()> {
    let mut group_index = 0usize;
    let mut non_group_index = 0usize;
    for mark in marks.iter_mut() {
        if mark.type_ == "group" {
            let mut nested_scope = Vec::from(scope);
            nested_scope.push(group_index as u32);
            let is_facet_group = mark
                .from
                .as_ref()
                .and_then(|from| from.facet.as_ref())
                .is_some();
            process_marks(
                &mut mark.marks,
                nested_scope.as_slice(),
                in_facet_group || is_facet_group,
                server_spec,
                task_scope,
                config,
            )?;
            group_index += 1;
        } else {
            process_non_group_mark(
                mark,
                scope,
                in_facet_group,
                non_group_index,
                server_spec,
                task_scope,
                config,
            )?;
            non_group_index += 1;
        }
    }
    Ok(())
}

fn process_non_group_mark(
    mark: &mut MarkSpec,
    scope: &[u32],
    in_facet_group: bool,
    non_group_index: usize,
    server_spec: &mut ChartSpec,
    task_scope: &mut TaskScope,
    config: &PlannerConfig,
) -> Result<()> {
    if in_facet_group {
        return Ok(());
    }

    let Some(source_data_name) = mark.from.as_ref().and_then(|from| from.data.clone()) else {
        return Ok(());
    };

    let mark_id = mark
        .name
        .clone()
        .unwrap_or_else(|| format!("mark_{non_group_index}"));

    let Some(encode) = &mut mark.encode else {
        return Ok(());
    };
    let Some(update_encodings) = encode.encodings.get_mut("update") else {
        return Ok(());
    };

    // Source data must currently be available on the server to safely precompute mark channels.
    let source_var = Variable::new_data(&source_data_name);
    let Ok(resolved_source) = task_scope.resolve_scope(&source_var, scope) else {
        return Ok(());
    };
    if server_spec
        .get_nested_data(&resolved_source.scope, &resolved_source.var.name)
        .is_err()
    {
        return Ok(());
    }

    let derived_dataset_name = make_unique_derived_dataset_name(scope, &mark_id, task_scope)?;

    let mut extracted_channels: Vec<MarkEncodingChannelSpec> = Vec::new();
    let mut replacements: Vec<(String, String)> = Vec::new();

    for channel_name in update_encodings.channels.keys().sorted() {
        let Some(channel_encoding) = update_encodings.channels.get(channel_name) else {
            continue;
        };
        if !mark_encoding_supported(channel_encoding) {
            continue;
        }
        if !channel_scales_supported_and_available(
            channel_encoding,
            scope,
            server_spec,
            task_scope,
            config,
        ) {
            continue;
        }

        let output_col = format!(
            "__vf_markenc_{}_{}",
            sanitize_name(&derived_dataset_name),
            sanitize_name(channel_name)
        );
        extracted_channels.push(MarkEncodingChannelSpec {
            channel: channel_name.clone(),
            as_: output_col.clone(),
            encoding: channel_encoding.clone(),
            extra: Default::default(),
        });
        replacements.push((channel_name.clone(), output_col));
    }

    if extracted_channels.is_empty() {
        return Ok(());
    }

    let derived_data = DataSpec {
        name: derived_dataset_name.clone(),
        source: Some(source_data_name),
        url: None,
        format: None,
        values: None,
        transform: vec![TransformSpec::MarkEncoding(MarkEncodingTransformSpec {
            encode_set: "update".to_string(),
            channels: extracted_channels,
            extra: Default::default(),
        })],
        on: None,
        extra: Default::default(),
    };

    server_spec.add_nested_data(scope, derived_data, None)?;
    task_scope.add_variable(&Variable::new_data(&derived_dataset_name), scope)?;

    if let Some(mark_from) = &mut mark.from {
        mark_from.data = Some(derived_dataset_name);
    }

    for (channel_name, output_col) in replacements {
        update_encodings.channels.insert(
            channel_name,
            MarkEncodingOrList::Scalar(Box::new(MarkEncodingSpec {
                value: None,
                field: Some(MarkEncodingField::Field(output_col)),
                scale: None,
                band: None,
                signal: None,
                test: None,
                offset: None,
                extra: Default::default(),
            })),
        );
    }

    Ok(())
}

fn channel_scales_supported_and_available(
    encoding: &MarkEncodingOrList,
    usage_scope: &[u32],
    server_spec: &ChartSpec,
    task_scope: &TaskScope,
    config: &PlannerConfig,
) -> bool {
    let Ok(input_vars) = mark_encoding_input_vars(encoding) else {
        return false;
    };

    for input_var in input_vars {
        if !matches!(input_var.var.ns(), VariableNamespace::Scale) {
            continue;
        }

        if !config.copy_scales_to_server {
            return false;
        }

        let Ok(resolved_scale) = task_scope.resolve_scope(&input_var.var, usage_scope) else {
            return false;
        };
        let Some(scale) =
            get_nested_scale(server_spec, &resolved_scale.scope, &resolved_scale.var.name)
        else {
            return false;
        };

        let scale_type = scale.type_.clone().unwrap_or_default();
        if !server_mark_encoding_scale_type_supported(&scale_type) {
            return false;
        }
    }

    true
}

fn get_nested_scale<'a>(spec: &'a ChartSpec, scope: &[u32], name: &str) -> Option<&'a ScaleSpec> {
    let scales = if scope.is_empty() {
        &spec.scales
    } else {
        &spec.get_nested_group(scope).ok()?.scales
    };

    scales.iter().find(|scale| scale.name == name)
}

fn server_mark_encoding_scale_type_supported(scale_type: &ScaleTypeSpec) -> bool {
    matches!(
        scale_type,
        ScaleTypeSpec::Linear
            | ScaleTypeSpec::Log
            | ScaleTypeSpec::Pow
            | ScaleTypeSpec::Sqrt
            | ScaleTypeSpec::Symlog
            | ScaleTypeSpec::Time
            | ScaleTypeSpec::Utc
            | ScaleTypeSpec::Band
            | ScaleTypeSpec::Point
            | ScaleTypeSpec::Ordinal
    )
}

fn make_unique_derived_dataset_name(
    scope: &[u32],
    mark_id: &str,
    task_scope: &TaskScope,
) -> Result<String> {
    let scope_str = if scope.is_empty() {
        "root".to_string()
    } else {
        scope.iter().map(|v| v.to_string()).join("_")
    };
    let base = format!(
        "_vf_markenc_{}_{}",
        sanitize_name(&scope_str),
        sanitize_name(mark_id)
    );

    let child = task_scope.get_child(scope)?;
    if !child.data.contains(&base) {
        return Ok(base);
    }

    for suffix in 1.. {
        let candidate = format!("{base}_{suffix}");
        if !child.data.contains(&candidate) {
            return Ok(candidate);
        }
    }

    unreachable!()
}

fn sanitize_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::extract_mark_encodings;
    use crate::planning::extract::extract_server_data;
    use crate::planning::plan::PlannerConfig;
    use crate::spec::chart::ChartSpec;
    use crate::spec::mark::MarkEncodingField;
    use crate::spec::transform::TransformSpec;
    use serde_json::json;

    fn chart_with_supported_and_unsupported_channels() -> ChartSpec {
        serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 2, "m": 1}, {"v": 7, "m": 0}],
                    "transform": [
                        {"type": "formula", "expr": "datum.v * 2", "as": "v2"}
                    ]
                }
            ],
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 10], "range": [0, 100]}
            ],
            "marks": [
                {
                    "type": "symbol",
                    "name": "pt",
                    "from": {"data": "source"},
                    "encode": {
                        "update": {
                            "x": {"field": "v", "scale": "x", "offset": 2},
                            "opacity": [
                                {"test": "datum.m > 0", "value": 1},
                                {"value": 0.2}
                            ],
                            "bad": {"field": {"group": "g"}}
                        }
                    }
                }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn test_extract_mark_encodings_rewrites_supported_channels_only() {
        let mut client_spec = chart_with_supported_and_unsupported_channels();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;
        config.precompute_mark_encodings = true;

        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();
        extract_mark_encodings(&mut client_spec, &mut server_spec, &mut task_scope, &config)
            .unwrap();

        let mark = &client_spec.marks[0];
        let source = mark.from.as_ref().and_then(|f| f.data.clone()).unwrap();
        assert!(source.starts_with("_vf_markenc_"));
        assert!(server_spec.data.iter().any(|d| d.name == source
            && matches!(d.transform.first(), Some(TransformSpec::MarkEncoding(_)))));

        let update = mark
            .encode
            .as_ref()
            .unwrap()
            .encodings
            .get("update")
            .unwrap();
        let x = update.channels.get("x").unwrap().to_vec();
        assert!(matches!(x[0].field, Some(MarkEncodingField::Field(_))));
        assert!(x[0].scale.is_none());

        let bad = update.channels.get("bad").unwrap().to_vec();
        assert!(matches!(bad[0].field, Some(MarkEncodingField::Object(_))));
    }

    #[test]
    fn test_extract_mark_encodings_scale_channels_require_copy_scales_flag() {
        let mut client_spec = chart_with_supported_and_unsupported_channels();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = false;
        config.precompute_mark_encodings = true;

        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();
        extract_mark_encodings(&mut client_spec, &mut server_spec, &mut task_scope, &config)
            .unwrap();

        let mark = &client_spec.marks[0];
        let source = mark.from.as_ref().and_then(|f| f.data.clone()).unwrap();
        assert!(source.starts_with("_vf_markenc_"));
        let update = mark
            .encode
            .as_ref()
            .unwrap()
            .encodings
            .get("update")
            .unwrap();

        let x = update.channels.get("x").unwrap().to_vec();
        assert_eq!(x[0].scale.as_deref(), Some("x"));

        let opacity = update.channels.get("opacity").unwrap().to_vec();
        assert!(matches!(
            opacity[0].field,
            Some(MarkEncodingField::Field(_))
        ));
        assert!(opacity[0].scale.is_none());

        let markenc_data = server_spec.data.iter().find(|d| d.name == source).unwrap();
        let TransformSpec::MarkEncoding(markenc) = markenc_data.transform.first().unwrap() else {
            panic!("expected mark_encoding transform");
        };
        assert_eq!(markenc.channels.len(), 1);
        assert_eq!(markenc.channels[0].channel, "opacity");
    }

    #[test]
    fn test_extract_mark_encodings_skip_channels_using_unsupported_scale_types() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 2, "m": 1}, {"v": 7, "m": 0}],
                    "transform": [
                        {"type": "formula", "expr": "datum.v * 2", "as": "v2"}
                    ]
                }
            ],
            "scales": [
                {"name": "x", "type": "sequential", "domain": [0, 10], "range": [0, 100]}
            ],
            "marks": [
                {
                    "type": "symbol",
                    "name": "pt",
                    "from": {"data": "source"},
                    "encode": {
                        "update": {
                            "x": {"field": "v", "scale": "x", "offset": 2},
                            "opacity": [
                                {"test": "datum.m > 0", "value": 1},
                                {"value": 0.2}
                            ]
                        }
                    }
                }
            ]
        }))
        .unwrap();
        let mut task_scope = client_spec.to_task_scope().unwrap();

        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;
        config.precompute_mark_encodings = true;

        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();
        extract_mark_encodings(&mut client_spec, &mut server_spec, &mut task_scope, &config)
            .unwrap();

        let mark = &client_spec.marks[0];
        let source = mark.from.as_ref().and_then(|f| f.data.clone()).unwrap();
        assert!(source.starts_with("_vf_markenc_"));
        let update = mark
            .encode
            .as_ref()
            .unwrap()
            .encodings
            .get("update")
            .unwrap();

        let x = update.channels.get("x").unwrap().to_vec();
        assert_eq!(x[0].scale.as_deref(), Some("x"));

        let opacity = update.channels.get("opacity").unwrap().to_vec();
        assert!(matches!(
            opacity[0].field,
            Some(MarkEncodingField::Field(_))
        ));
        assert!(opacity[0].scale.is_none());

        let markenc_data = server_spec.data.iter().find(|d| d.name == source).unwrap();
        let TransformSpec::MarkEncoding(markenc) = markenc_data.transform.first().unwrap() else {
            panic!("expected mark_encoding transform");
        };
        assert_eq!(markenc.channels.len(), 1);
        assert_eq!(markenc.channels[0].channel, "opacity");
    }

    #[test]
    fn test_extract_mark_encodings_skips_facet_group_marks() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {"name": "source", "values": [{"g":"a", "v": 2}, {"g":"b", "v": 7}]}
            ],
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 10], "range": [0, 100]}
            ],
            "marks": [
                {
                    "type": "group",
                    "from": {"facet": {"data": "source", "name": "facet_data", "groupby": "g"}},
                    "marks": [
                        {
                            "type": "symbol",
                            "from": {"data": "facet_data"},
                            "encode": {"update": {"x": {"field": "v", "scale": "x"}}}
                        }
                    ]
                }
            ]
        }))
        .unwrap();

        let mut task_scope = client_spec.to_task_scope().unwrap();
        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;
        config.precompute_mark_encodings = true;
        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();

        extract_mark_encodings(&mut client_spec, &mut server_spec, &mut task_scope, &config)
            .unwrap();

        let child = &client_spec.marks[0].marks[0];
        assert_eq!(
            child.from.as_ref().and_then(|f| f.data.clone()).as_deref(),
            Some("facet_data")
        );
        assert!(!server_spec
            .get_nested_group(&[0])
            .unwrap()
            .data
            .iter()
            .any(|d| d.name.starts_with("_vf_markenc_")));
    }

    #[test]
    fn test_extract_mark_encodings_skips_when_mark_source_not_on_server() {
        let mut client_spec: ChartSpec = serde_json::from_value(json!({
            "$schema": "https://vega.github.io/schema/vega/v5.json",
            "data": [
                {
                    "name": "source",
                    "values": [{"v": 1}],
                    "transform": [{"type": "cross"}]
                }
            ],
            "scales": [
                {"name": "x", "type": "linear", "domain": [0, 10], "range": [0, 100]}
            ],
            "marks": [
                {
                    "type": "symbol",
                    "from": {"data": "source"},
                    "encode": {"update": {"x": {"field": "v", "scale": "x"}}}
                }
            ]
        }))
        .unwrap();

        let mut task_scope = client_spec.to_task_scope().unwrap();
        let mut config = PlannerConfig::default();
        config.extract_inline_data = true;
        config.copy_scales_to_server = true;
        config.precompute_mark_encodings = true;
        let mut server_spec =
            extract_server_data(&mut client_spec, &mut task_scope, &config).unwrap();

        // source is unsupported, so nothing is extracted to server and no mark rewrite should occur.
        assert!(server_spec.data.is_empty());
        extract_mark_encodings(&mut client_spec, &mut server_spec, &mut task_scope, &config)
            .unwrap();

        assert_eq!(
            client_spec.marks[0]
                .from
                .as_ref()
                .and_then(|f| f.data.as_deref()),
            Some("source")
        );
        assert!(server_spec
            .data
            .iter()
            .all(|d| !d.name.starts_with("_vf_markenc_")));
    }
}
