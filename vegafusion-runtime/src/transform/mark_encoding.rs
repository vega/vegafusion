use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::transform::TransformTrait;
use async_trait::async_trait;
use datafusion::prelude::DataFrame;
use datafusion_expr::expr::{BinaryExpr, Case, ScalarFunction};
use datafusion_expr::{lit, Expr, Operator};
use datafusion_functions::math::round;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::datafusion_common::{DFSchema, ScalarValue};
use vegafusion_common::datatypes::{to_boolean, to_numeric};
use vegafusion_common::error::{Result, ResultWithContext, VegaFusionError};
use vegafusion_core::expression::parser::parse;
use vegafusion_core::proto::gen::transforms::{MarkEncoding, MarkEncodingChannel};
use vegafusion_core::spec::mark::{
    EncodingOffset, MarkEncodingField, MarkEncodingFieldObject, MarkEncodingOrList,
    MarkEncodingSpec,
};
use vegafusion_core::task_graph::task_value::TaskValue;

#[cfg(feature = "scales")]
use crate::{datafusion::udfs::scale::make_scale_udf, scale::adapter::scale_bandwidth};

#[async_trait]
impl TransformTrait for MarkEncoding {
    async fn eval(
        &self,
        dataframe: DataFrame,
        config: &CompilationConfig,
    ) -> Result<(DataFrame, Vec<TaskValue>)> {
        let schema = dataframe.schema();

        if std::env::var_os("VF_DEBUG_MARKENC_SCALES").is_some() {
            eprintln!(
                "mark_encoding encode_set={} channels={} scale_scope={:?}",
                self.encode_set,
                self.channels.len(),
                config.scale_scope.keys().collect::<Vec<_>>()
            );
        }

        let mut computed_cols: Vec<(String, Expr)> = Vec::new();
        for channel in &self.channels {
            let expr = compile_channel_expr(channel, config, schema).with_context(|| {
                format!(
                    "Failed to compile mark_encoding channel {}",
                    channel.channel
                )
            })?;
            computed_cols.push((channel.r#as.clone(), expr.alias(&channel.r#as)));
        }

        let mut selections: Vec<Expr> =
            schema.fields().iter().map(|f| flat_col(f.name())).collect();
        for (as_name, expr) in computed_cols {
            if let Some(idx) = schema.fields().iter().position(|f| f.name() == &as_name) {
                selections[idx] = expr;
            } else {
                selections.push(expr);
            }
        }

        let result = dataframe.select(selections).with_context(|| {
            format!(
                "mark_encoding transform failed for encode set {}",
                self.encode_set
            )
        })?;
        Ok((result, Default::default()))
    }
}

fn compile_channel_expr(
    channel: &MarkEncodingChannel,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    let encoding: MarkEncodingOrList =
        serde_json::from_str(&channel.encoding_json).with_context(|| {
            format!(
                "Failed to deserialize MarkEncodingOrList for channel {}",
                channel.channel
            )
        })?;

    let mut expr = lit(ScalarValue::Null);
    for rule in encoding.to_vec().into_iter().rev() {
        let value_expr = compile_rule_value(&rule, config, schema)?;
        if let Some(test_expr_str) = &rule.test {
            let parsed = parse(test_expr_str)?;
            let test_expr = compile(&parsed, config, Some(schema))?;
            let test_expr = to_boolean(test_expr, schema)?;
            expr = Expr::Case(Case {
                expr: None,
                when_then_expr: vec![(Box::new(test_expr), Box::new(value_expr))],
                else_expr: Some(Box::new(expr)),
            });
        } else {
            expr = value_expr;
        }
    }

    Ok(expr)
}

fn compile_rule_value(
    rule: &MarkEncodingSpec,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    validate_rule_supported(rule)?;

    let (base_value_expr, has_base_value) = compile_base_value(rule, config, schema)?;
    let mut value_expr = if has_base_value {
        Some(base_value_expr)
    } else {
        None
    };

    if let Some(scale_name) = &rule.scale {
        value_expr = value_expr
            .map(|expr| apply_scale(expr, scale_name, config))
            .transpose()?;

        if let Some(band) = band_factor(rule)? {
            let band_expr = apply_band_offset(scale_name, band, config)?;
            value_expr = Some(match value_expr {
                Some(expr) => add_numeric(expr, band_expr, schema)?,
                None => band_expr,
            });
        }

        // Vega value refs with `scale` and no base value (for example `{"scale":"x","band":1}`)
        // resolve to 0 / bandwidth forms rather than null.
        if value_expr.is_none() {
            value_expr = Some(lit(ScalarValue::from(0.0)));
        }
    }

    let mut value_expr = value_expr.unwrap_or_else(|| lit(ScalarValue::Null));

    if let Some(mult) = rule.extra.get("mult") {
        let mult_expr = compile_json_or_signal(mult, config, schema)?;
        value_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_numeric(value_expr, schema)?),
            op: Operator::Multiply,
            right: Box::new(to_numeric(mult_expr, schema)?),
        });
    }

    if let Some(offset) = &rule.offset {
        let offset_expr = match offset {
            EncodingOffset::Value(value) => compile_json_or_signal(value, config, schema)?,
            EncodingOffset::Encoding(offset_encoding) => {
                compile_rule_value(offset_encoding, config, schema)?
            }
        };
        value_expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(to_numeric(value_expr, schema)?),
            op: Operator::Plus,
            right: Box::new(to_numeric(offset_expr, schema)?),
        });
    }

    if let Some(round_spec) = rule.extra.get("round") {
        let numeric_expr = to_numeric(value_expr.clone(), schema)?;
        let rounded_expr = Expr::ScalarFunction(ScalarFunction {
            func: round(),
            args: vec![numeric_expr.clone()],
        });
        value_expr = if let Some(round_enabled) = round_spec.as_bool() {
            if round_enabled {
                rounded_expr
            } else {
                numeric_expr
            }
        } else if is_signal_object(round_spec) {
            let round_when =
                to_boolean(compile_json_or_signal(round_spec, config, schema)?, schema)?;
            Expr::Case(Case {
                expr: None,
                when_then_expr: vec![(Box::new(round_when), Box::new(rounded_expr))],
                else_expr: Some(Box::new(numeric_expr)),
            })
        } else {
            return Err(VegaFusionError::internal(
                "mark_encoding round must be a boolean or signal expression object",
            ));
        };
    }

    Ok(value_expr)
}

fn compile_base_value(
    rule: &MarkEncodingSpec,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<(Expr, bool)> {
    if let Some(signal) = &rule.signal {
        let parsed = parse(signal)?;
        return Ok((compile(&parsed, config, Some(schema))?, true));
    }

    if let Some(field) = &rule.field {
        return Ok((compile_field_expr(field, schema)?, true));
    }

    if let Some(value) = &rule.value {
        return Ok((lit(ScalarValue::from_json(value)?), true));
    }

    Ok((lit(ScalarValue::Null), false))
}

fn compile_field_expr(field: &MarkEncodingField, schema: &DFSchema) -> Result<Expr> {
    match field {
        MarkEncodingField::Field(name) => compile_simple_field_expr(name, schema),
        MarkEncodingField::Object(MarkEncodingFieldObject {
            datum,
            group,
            parent,
            signal,
            extra,
        }) => {
            if signal.is_some() || group.is_some() || parent.is_some() || !extra.is_empty() {
                return Err(VegaFusionError::internal(
                    "mark_encoding field objects with signal/group/parent/extra are not supported",
                ));
            }
            let Some(datum_field) = datum else {
                return Err(VegaFusionError::internal(
                    "mark_encoding field object must contain datum in this phase",
                ));
            };
            compile_simple_field_expr(datum_field, schema)
        }
    }
}

fn compile_simple_field_expr(field: &str, schema: &DFSchema) -> Result<Expr> {
    let unescaped = vegafusion_common::escape::unescape_field(field);
    if schema.field_with_unqualified_name(&unescaped).is_ok() {
        Ok(unescaped_col(field))
    } else {
        if std::env::var_os("VF_DEBUG_MARKENC_FIELDS").is_some() {
            let fields = schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<_>>();
            eprintln!(
                "mark_encoding missing field {:?} (unescaped {:?}); schema fields={:?}",
                field, unescaped, fields
            );
        }
        // Match Vega's undefined-ish behavior as SQL null when a field is missing.
        Ok(lit(ScalarValue::Null))
    }
}

fn compile_json_or_signal(
    value: &serde_json::Value,
    config: &CompilationConfig,
    schema: &DFSchema,
) -> Result<Expr> {
    if let Some(signal) = value
        .as_object()
        .and_then(|obj| obj.get("signal"))
        .and_then(|v| v.as_str())
    {
        let parsed = parse(signal)?;
        compile(&parsed, config, Some(schema))
    } else {
        Ok(lit(ScalarValue::from_json(value)?))
    }
}

fn validate_rule_supported(rule: &MarkEncodingSpec) -> Result<()> {
    if let Some(band) = &rule.band {
        if band.as_f64().is_none_or(|v| !v.is_finite()) {
            return Err(VegaFusionError::internal(
                "mark_encoding band must be a finite numeric value",
            ));
        }
        if rule.scale.is_none() {
            return Err(VegaFusionError::internal(
                "mark_encoding band value refs require a scale",
            ));
        }
    }

    for key in rule.extra.keys() {
        if !matches!(key.as_str(), "mult" | "round") {
            return Err(VegaFusionError::internal(format!(
                "mark_encoding does not support value-ref key {key:?} in this phase"
            )));
        }
    }

    if let Some(field) = &rule.field {
        if let MarkEncodingField::Object(obj) = field {
            if obj.signal.is_some()
                || obj.group.is_some()
                || obj.parent.is_some()
                || obj.datum.is_none()
                || !obj.extra.is_empty()
            {
                return Err(VegaFusionError::internal(
                    "mark_encoding field objects are only supported with datum references in this phase",
                ));
            }
        }
    }

    Ok(())
}

fn band_factor(rule: &MarkEncodingSpec) -> Result<Option<f64>> {
    let Some(band) = &rule.band else {
        return Ok(None);
    };
    let Some(factor) = band.as_f64() else {
        return Err(VegaFusionError::internal(
            "mark_encoding band must be a finite numeric value",
        ));
    };
    if !factor.is_finite() {
        return Err(VegaFusionError::internal(
            "mark_encoding band must be a finite numeric value",
        ));
    }
    if factor == 0.0 {
        Ok(None)
    } else {
        Ok(Some(factor))
    }
}

fn add_numeric(lhs: Expr, rhs: Expr, schema: &DFSchema) -> Result<Expr> {
    Ok(Expr::BinaryExpr(BinaryExpr {
        left: Box::new(to_numeric(lhs, schema)?),
        op: Operator::Plus,
        right: Box::new(to_numeric(rhs, schema)?),
    }))
}

#[cfg(feature = "scales")]
fn apply_scale(value_expr: Expr, scale_name: &str, config: &CompilationConfig) -> Result<Expr> {
    let Some(scale_state) = config.scale_scope.get(scale_name) else {
        return Ok(lit(ScalarValue::Null));
    };

    let udf = make_scale_udf(scale_name, false, scale_state, &config.tz_config)?;
    Ok(Expr::ScalarFunction(ScalarFunction {
        func: udf,
        args: vec![value_expr],
    }))
}

#[cfg(not(feature = "scales"))]
fn apply_scale(_value_expr: Expr, _scale_name: &str, _config: &CompilationConfig) -> Result<Expr> {
    Err(VegaFusionError::internal(
        "mark_encoding scale evaluation requires the vegafusion-runtime `scales` feature",
    ))
}

#[cfg(feature = "scales")]
fn apply_band_offset(scale_name: &str, band: f64, config: &CompilationConfig) -> Result<Expr> {
    let bandwidth = config
        .scale_scope
        .get(scale_name)
        .map(|scale_state| scale_bandwidth(scale_state, &config.tz_config))
        .transpose()?
        .unwrap_or(0.0);

    Ok(lit(ScalarValue::from(bandwidth * band)))
}

#[cfg(not(feature = "scales"))]
fn apply_band_offset(_scale_name: &str, _band: f64, _config: &CompilationConfig) -> Result<Expr> {
    Err(VegaFusionError::internal(
        "mark_encoding scale evaluation requires the vegafusion-runtime `scales` feature",
    ))
}

fn is_signal_object(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .map(|obj| obj.len() == 1 && obj.contains_key("signal"))
        .unwrap_or(false)
}
