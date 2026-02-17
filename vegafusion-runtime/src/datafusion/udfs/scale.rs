use crate::scale::adapter::to_configured_scale;
use crate::task_graph::timezone::RuntimeTzConfig;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use vegafusion_common::arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, FixedSizeListArray, StringArray,
};
use vegafusion_common::arrow::compute::cast;
use vegafusion_common::arrow::datatypes::{DataType, Field};
use vegafusion_common::datafusion_common::{DataFusionError, Result as DFResult, ScalarValue};
use vegafusion_common::datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use vegafusion_core::error::Result;
use vegafusion_core::task_graph::scale_state::ScaleState;

use avenger_scales::scales::ConfiguredScale;

pub fn make_scale_udf(
    scale_name: &str,
    invert: bool,
    scale_state: &ScaleState,
    tz_config: &Option<RuntimeTzConfig>,
) -> Result<Arc<ScalarUDF>> {
    let configured_scale = Arc::new(to_configured_scale(scale_state, tz_config)?);
    let udf = ScaleExprUDF::new(scale_name, invert, configured_scale);
    Ok(Arc::new(ScalarUDF::from(udf)))
}

#[derive(Debug, Clone)]
struct ScaleExprUDF {
    signature: Signature,
    scale_name: String,
    invert: bool,
    configured_scale: Arc<ConfiguredScale>,
    output_scalar_type: DataType,
}

impl ScaleExprUDF {
    fn new(scale_name: &str, invert: bool, configured_scale: Arc<ConfiguredScale>) -> Self {
        let output_scalar_type = infer_output_scalar_type(&configured_scale, invert);

        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
            scale_name: scale_name.to_string(),
            invert,
            configured_scale,
            output_scalar_type,
        }
    }

    fn fn_name(&self) -> &'static str {
        if self.invert {
            "invert"
        } else {
            "scale"
        }
    }

    fn apply_to_array(
        &self,
        values: &ArrayRef,
        is_interval_list_input: bool,
    ) -> DFResult<ArrayRef> {
        if self.invert {
            if is_interval_list_input {
                self.apply_invert_interval(values)
            } else {
                self.configured_scale.invert(values).map_err(|err| {
                    DataFusionError::Execution(format!(
                        "Failed to evaluate invert('{}', ...): {err}",
                        self.scale_name
                    ))
                })
            }
        } else {
            let scaled = self.configured_scale.scale(values).map_err(|err| {
                DataFusionError::Execution(format!(
                    "Failed to evaluate scale('{}', ...): {err}",
                    self.scale_name
                ))
            })?;
            color_array_to_css_strings_if_needed(scaled)
        }
    }

    fn apply_invert_interval(&self, values: &ArrayRef) -> DFResult<ArrayRef> {
        if values.len() == 2 && !values.is_null(0) && !values.is_null(1) {
            let cast_values = cast(values, &DataType::Float32).map_err(|err| {
                DataFusionError::Execution(format!(
                    "Failed to coerce invert('{}', interval) input to Float32: {err}",
                    self.scale_name
                ))
            })?;
            let primitive =
                cast_values.as_primitive::<vegafusion_common::arrow::datatypes::Float32Type>();
            let lo = primitive.value(0);
            let hi = primitive.value(1);

            if let Ok(interval_result) = self.configured_scale.invert_range_interval((lo, hi)) {
                return Ok(interval_result);
            }
        }

        self.configured_scale.invert(values).map_err(|err| {
            DataFusionError::Execution(format!(
                "Failed to evaluate invert('{}', ...): {err}",
                self.scale_name
            ))
        })
    }

    fn make_null_scalar(&self) -> DFResult<ScalarValue> {
        ScalarValue::try_from(&self.output_scalar_type)
    }

    fn output_type_for_input(&self, input_type: &DataType) -> DataType {
        let item_field = Arc::new(Field::new("item", self.output_scalar_type.clone(), true));
        match input_type {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                DataType::List(item_field)
            }
            _ => self.output_scalar_type.clone(),
        }
    }
}

fn infer_output_scalar_type(configured_scale: &ConfiguredScale, invert: bool) -> DataType {
    let fallback = if invert {
        configured_scale.domain().data_type().clone()
    } else {
        configured_scale.range().data_type().clone()
    };

    let sample_input = if invert {
        if configured_scale.range().is_empty() {
            new_empty_array(configured_scale.range().data_type())
        } else {
            configured_scale.range().slice(0, 1)
        }
    } else if configured_scale.domain().is_empty() {
        new_empty_array(configured_scale.domain().data_type())
    } else {
        configured_scale.domain().slice(0, 1)
    };

    let output = if invert {
        configured_scale.invert(&sample_input)
    } else {
        configured_scale.scale(&sample_input)
    };
    let output_type = output
        .map(|arr| arr.data_type().clone())
        .unwrap_or(fallback);
    if !invert && looks_like_color_output_type(&output_type) {
        DataType::Utf8
    } else {
        output_type
    }
}

fn color_array_to_css_strings_if_needed(values: ArrayRef) -> DFResult<ArrayRef> {
    if !looks_like_color_output_type(values.data_type()) {
        return Ok(values);
    }

    let mut css_values: Vec<Option<String>> = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        if values.is_null(row) {
            css_values.push(None);
            continue;
        }

        let scalar = ScalarValue::try_from_array(values.as_ref(), row)?;
        let rgba = scalar_to_rgba(&scalar)?;
        css_values.push(Some(rgba_to_hex(rgba)));
    }
    Ok(Arc::new(StringArray::from(css_values)))
}

fn looks_like_color_output_type(dtype: &DataType) -> bool {
    match dtype {
        DataType::FixedSizeList(field, n) => *n == 4 && is_color_component_type(field.data_type()),
        DataType::List(field) | DataType::LargeList(field) => {
            is_color_component_type(field.data_type())
        }
        _ => false,
    }
}

fn is_color_component_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

fn scalar_to_rgba(value: &ScalarValue) -> DFResult<[f64; 4]> {
    let values = scalar_list_values(value)?.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Expected list-like color output from scale(), received {value:?}"
        ))
    })?;

    if values.len() < 3 {
        return Err(DataFusionError::Execution(format!(
            "Expected color output with at least RGB components, received length {}",
            values.len()
        )));
    }

    let r = scalar_to_f64(&ScalarValue::try_from_array(values.as_ref(), 0)?)?;
    let g = scalar_to_f64(&ScalarValue::try_from_array(values.as_ref(), 1)?)?;
    let b = scalar_to_f64(&ScalarValue::try_from_array(values.as_ref(), 2)?)?;
    let a = if values.len() >= 4 {
        scalar_to_f64(&ScalarValue::try_from_array(values.as_ref(), 3)?)?
    } else {
        1.0
    };
    Ok([r, g, b, a])
}

fn scalar_to_f64(value: &ScalarValue) -> DFResult<f64> {
    match value {
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Float32(Some(v)) => Ok(*v as f64),
        ScalarValue::Float16(Some(v)) => Ok(f64::from(*v)),
        ScalarValue::Int8(Some(v)) => Ok(*v as f64),
        ScalarValue::Int16(Some(v)) => Ok(*v as f64),
        ScalarValue::Int32(Some(v)) => Ok(*v as f64),
        ScalarValue::Int64(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt8(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as f64),
        ScalarValue::UInt64(Some(v)) => Ok(*v as f64),
        _ => Err(DataFusionError::Execution(format!(
            "Expected numeric color component, received {value:?}"
        ))),
    }
}

fn rgba_to_hex([r, g, b, a]: [f64; 4]) -> String {
    let r = (r.clamp(0.0, 1.0) * 255.0).round() as u8;
    let g = (g.clamp(0.0, 1.0) * 255.0).round() as u8;
    let b = (b.clamp(0.0, 1.0) * 255.0).round() as u8;
    let a = (a.clamp(0.0, 1.0) * 255.0).round() as u8;
    if a == u8::MAX {
        format!("#{r:02x}{g:02x}{b:02x}")
    } else {
        format!("#{r:02x}{g:02x}{b:02x}{a:02x}")
    }
}

impl PartialEq for ScaleExprUDF {
    fn eq(&self, other: &Self) -> bool {
        self.scale_name == other.scale_name
            && self.invert == other.invert
            && Arc::ptr_eq(&self.configured_scale, &other.configured_scale)
    }
}

impl Eq for ScaleExprUDF {}

impl Hash for ScaleExprUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.scale_name.hash(state);
        self.invert.hash(state);
        Arc::as_ptr(&self.configured_scale).hash(state);
    }
}

impl ScalarUDFImpl for ScaleExprUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        if self.invert {
            "vf_invert"
        } else {
            "vf_scale"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.output_scalar_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<Arc<Field>> {
        let Some(arg0) = args.arg_fields.first() else {
            return Err(DataFusionError::Execution(format!(
                "{} requires a single argument",
                self.fn_name()
            )));
        };
        let dtype = self.output_type_for_input(arg0.data_type());
        Ok(Arc::new(Field::new(self.name(), dtype, true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = args.args;
        if args.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "{} requires one input value argument",
                self.fn_name()
            )));
        }

        match &args[0] {
            ColumnarValue::Array(values) => {
                if matches!(
                    values.data_type(),
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
                ) {
                    return Err(DataFusionError::Execution(format!(
                        "{}('{}', array-of-arrays) is not supported in this phase",
                        self.fn_name(),
                        self.scale_name
                    )));
                }

                let scaled = self.apply_to_array(values, false)?;
                Ok(ColumnarValue::Array(scaled))
            }
            ColumnarValue::Scalar(value) => {
                if value.is_null() {
                    return self.make_null_scalar().map(ColumnarValue::Scalar);
                }

                if let Some(list_values) = scalar_list_values(value)? {
                    let scaled = self.apply_to_array(&list_values, self.invert)?;
                    let wrapped = ScalarValue::List(Arc::new(
                        datafusion_common::utils::SingleRowListArrayBuilder::new(scaled)
                            .with_nullable(true)
                            .build_list_array(),
                    ));
                    return Ok(ColumnarValue::Scalar(wrapped));
                }

                let scalar_arr = value.to_array()?;
                let scaled = self.apply_to_array(&scalar_arr, false)?;
                let scalar = ScalarValue::try_from_array(&scaled, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }
}

fn scalar_list_values(value: &ScalarValue) -> DFResult<Option<ArrayRef>> {
    let result = match value {
        ScalarValue::List(arr) => {
            if arr.is_empty() {
                Some(new_empty_array(&arr.value_type()))
            } else {
                Some(arr.value(0))
            }
        }
        ScalarValue::LargeList(arr) => {
            if arr.is_empty() {
                Some(new_empty_array(&arr.value_type()))
            } else {
                Some(arr.value(0))
            }
        }
        ScalarValue::FixedSizeList(arr) => {
            if arr.is_empty() {
                Some(new_empty_array(&arr.value_type()))
            } else {
                let arr = arr
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "Failed to downcast FixedSizeList scalar argument".to_string(),
                        )
                    })?;
                Some(arr.value(0))
            }
        }
        _ => None,
    };
    Ok(result)
}
