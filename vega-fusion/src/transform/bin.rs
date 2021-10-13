use crate::error::{Result, ResultWithContext, VegaFusionError};
use crate::expression::ast::array::ArrayExpression;
use crate::expression::ast::base::Expression;
use crate::expression::ast::literal::{Literal, LiteralValue};
use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::{ExprHelpers, ScalarValueHelpers};
use crate::expression::parser::parse;
use crate::spec::transform::bin::{BinExtent, BinTransformSpec};
use crate::spec::values::SignalExpressionSpec;
use crate::transform::base::TransformTrait;
use crate::variable::Variable;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::compute::unary;
use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::{col, lit, Expr};
use datafusion::physical_plan::functions::{
    make_scalar_function, ReturnTypeFunction, Signature, Volatility,
};
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::scalar::ScalarValue;
use float_cmp::approx_eq;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, Hash)]
pub struct BinTransform {
    pub field: String,
    pub extent_expr: Expression,
    pub as_: Vec<String>,
    pub input_vars: Vec<Variable>,
    pub signal: Option<String>,
    pub config: BinConfig,
}

impl BinTransform {
    pub fn try_new(transform: &BinTransformSpec) -> Result<Self> {
        let field = transform.field.field();

        let extent_expr = match &transform.extent {
            BinExtent::Value(extent) => {
                // Convert extent value to an expression for consistency
                Expression::from(ArrayExpression {
                    elements: vec![
                        Expression::from(Literal {
                            value: LiteralValue::Number(extent[0]),
                            raw: extent[0].to_string(),
                            span: None,
                        }),
                        Expression::from(Literal {
                            value: LiteralValue::Number(extent[1]),
                            raw: extent[1].to_string(),
                            span: None,
                        }),
                    ],
                    span: None,
                })
            }
            BinExtent::Signal(SignalExpressionSpec { signal }) => parse(signal)?,
        };

        let config = BinConfig::from_spec(transform.clone());

        let input_vars = extent_expr.get_variables();

        Ok(Self {
            field,
            extent_expr,
            as_: transform.as_.clone().unwrap_or_default(),
            input_vars,
            signal: transform.signal.clone(),
            config,
        })
    }
}

impl TransformTrait for BinTransform {
    fn call(
        &self,
        dataframe: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<ScalarValue>)> {
        // Compute extent
        let expr = compile(&self.extent_expr, config, Some(dataframe.schema()))?;
        let extent_scalar = expr.eval_to_scalar()?;
        let extent = extent_scalar.to_f64x2()?;

        // Compute binning solution
        let params = calculate_bin_params(&extent, &self.config)?;
        println!("extent: {:?}\n params: {:?}", extent, params);
        let BinParams {
            start,
            stop,
            step,
            n,
        } = params;
        let bin_starts: Vec<f64> = (0..n).map(|i| start + step * i as f64).collect();

        // Compute output signal value
        let mut fname = self.field.clone();
        fname.insert_str(0, "bin_");

        let fields = ScalarValue::List(
            Some(Box::new(vec![ScalarValue::from(self.field.as_str())])),
            Box::new(DataType::Utf8),
        );
        let output_value = if self.signal.is_some() {
            Some(ScalarValue::from(vec![
                ("fields", fields),
                ("fname", ScalarValue::from(fname.as_str())),
                ("start", ScalarValue::from(start)),
                ("step", ScalarValue::from(step)),
                ("stop", ScalarValue::from(stop)),
            ]))
        } else {
            None
        };

        // Investigate: Would it be faster to define this function once and input the binning
        // parameters?
        //
        // Implementation handles Float64 and Int64 separately to avoid having DataFusion
        // copy the full integer array into a float array. This improves performance on integer
        // columns, but this should be extended to the other numeric types as well.
        let bin = move |args: &[ArrayRef]| {
            let arg = &args[0];
            let dtype = arg.data_type();
            let binned_values = match dtype {
                DataType::Float64 => {
                    let field_values = args[0].as_any().downcast_ref::<Float64Array>().unwrap();
                    let binned_values: Float64Array = unary(field_values, |v| {
                        let bin_ind = (0.0 + (v - start) / step).floor() as i32;
                        if bin_ind < 0 {
                            f64::NEG_INFINITY
                        } else if bin_ind >= n {
                            f64::INFINITY
                        } else {
                            bin_starts[bin_ind as usize]
                        }
                    });
                    binned_values
                }
                DataType::Int64 => {
                    let field_values = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
                    let binned_values: Float64Array = unary(field_values, |v| {
                        let v = v as f64;
                        let bin_val = (v - start) / step;
                        let bin_ind = bin_val.floor() as i32;
                        if approx_eq!(f64, bin_val, n as f64, ulps = 1) {
                            // Close the right-hand edge of the top bin
                            bin_starts[(n - 1) as usize]
                        } else if bin_ind < 0 {
                            f64::NEG_INFINITY
                        } else if bin_ind >= n {
                            f64::INFINITY
                        } else {
                            bin_starts[bin_ind as usize]
                        }
                    });
                    binned_values
                }
                _ => unreachable!(),
            };

            Ok(Arc::new(binned_values) as ArrayRef)
        };
        let bin = make_scalar_function(bin);

        let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
        let bin = ScalarUDF::new(
            "bin",
            &Signature::uniform(
                1,
                vec![DataType::Float64, DataType::Int64],
                Volatility::Immutable,
            ),
            &return_type,
            &bin,
        );

        let bin_start = bin.call(vec![col(&self.field)]);

        // Name binned columns
        let (bin_start, name) = if let Some(as0) = self.as_.get(0) {
            (bin_start.alias(as0), as0.to_string())
        } else {
            (bin_start.alias("bin0"), "bin0".to_string())
        };

        let dataframe = dataframe
            .select(vec![Expr::Wildcard, bin_start])
            .with_context(|| "Failed to evaluate binning transform".to_string())?;

        // Split end into a separate select so that DataFusion knows to offset from previously
        // computed bin start, rather than recompute it.
        let bin_end = col(&name) + lit(step);
        let bin_end = if let Some(as1) = self.as_.get(1) {
            bin_end.alias(as1)
        } else {
            bin_end.alias("bin1")
        };

        let dataframe = dataframe
            .select(vec![Expr::Wildcard, bin_end])
            .with_context(|| "Failed to evaluate binning transform".to_string())?;

        Ok((dataframe.clone(), output_value.into_iter().collect()))
    }

    fn input_vars(&self) -> Vec<Variable> {
        self.input_vars.clone()
    }

    fn output_signals(&self) -> Vec<String> {
        self.signal.clone().into_iter().collect()
    }
}

// Port of https://github.com/vega/vega/blob/v5.9.1/packages/vega-statistics/src/bin.js
// with credit to
// https://github.com/altair-viz/altair-transform/blob/master/altair_transform/transform/vega_utils.py
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinConfig {
    /// A value in the binned domain at which to anchor the bins The bin boundaries will be shifted,
    /// if necessary, to ensure that a boundary aligns with the anchor value.
    anchor: Option<f64>,

    /// The number base to use for automatic bin selection (e.g. base 10)
    base: f64,

    /// Scale factors indicating the allowed subdivisions. The defualt value is vec![5.0, 2.0],
    /// which indicates that for base 10 numbers, the method may consider dividing bin sizes by 5
    /// and/or 2.
    divide: Vec<f64>,

    /// The maximum number of bins allowed
    maxbins: f64,

    /// A minimum distance between adjacent bins
    minstep: f64,

    /// If true, attempt to make the bin boundaries use human-friendly boundaries
    /// (e.g. whole numbers, multiples of 10, etc.)
    nice: bool,

    /// An exact step size to use between bins. Overrides other options.
    step: Option<f64>,

    /// A list of allowable step sizes to choose from
    steps: Option<Vec<f64>>,

    /// The value span over which to generate bin boundaries. Defaults to the exact extent of the
    /// data
    span: Option<f64>,
}

impl Default for BinConfig {
    fn default() -> Self {
        Self {
            anchor: None,
            base: 10.0,
            divide: vec![5.0, 2.0],
            maxbins: 20.0,
            minstep: 0.0,
            nice: true,
            step: None,
            steps: None,
            span: None,
        }
    }
}

impl BinConfig {
    pub fn from_spec(spec: BinTransformSpec) -> Self {
        let dflt = Self::default();
        Self {
            anchor: spec.anchor,
            base: spec.base.unwrap_or(dflt.base),
            divide: spec.divide.unwrap_or(dflt.divide),
            maxbins: spec.maxbins.unwrap_or(dflt.maxbins),
            minstep: spec.minstep.unwrap_or(dflt.minstep),
            nice: spec.nice.unwrap_or(dflt.nice),
            step: spec.step,
            steps: spec.steps,
            span: spec.span,
        }
    }
}

/// Custom Hash implementation that uses OrderedFloat to handle f64 fields.
impl Hash for BinConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // anchor
        self.anchor.map(OrderedFloat).hash(state);

        // base
        OrderedFloat(self.base).hash(state);

        // divide
        let divide: Vec<_> = self.divide.clone().into_iter().map(OrderedFloat).collect();
        divide.hash(state);

        // maxbins
        OrderedFloat(self.maxbins).hash(state);

        // minstep
        OrderedFloat(self.minstep).hash(state);

        // nice
        self.nice.hash(state);

        // step
        self.step.map(OrderedFloat).hash(state);

        // steps
        let steps: Option<Vec<_>> = self
            .steps
            .as_ref()
            .map(|v| v.clone().into_iter().map(OrderedFloat).collect());
        steps.hash(state);

        // span
        self.span.map(OrderedFloat).hash(state);
    }
}

#[derive(Clone, Debug)]
pub struct BinParams {
    pub start: f64,
    pub stop: f64,
    pub step: f64,
    pub n: i32,
}

pub fn calculate_bin_params(extent: &[f64; 2], config: &BinConfig) -> Result<BinParams> {
    let [min_, max_] = *extent;
    if min_ > max_ {
        return Err(VegaFusionError::specification(&format!(
            "extent[1] must be greater than extent[0]: Received {:?}",
            extent
        )));
    }

    let logb = config.base.ln();

    // Compute span
    let span = config.span.unwrap_or_else(|| {
        if !approx_eq!(f64, min_, max_) {
            max_ - min_
        } else if !approx_eq!(f64, min_, 0.0) {
            min_.abs()
        } else {
            1.0
        }
    });

    let step = if let Some(step) = config.step {
        // Use provided step as-is
        step
    } else if let Some(steps) = config.steps.as_ref().filter(|steps| !steps.is_empty()) {
        // If steps is provided, limit step to one of the elements.
        // Choose the first element of steps that will result in fewer than maxmins
        let min_step_size = span / config.maxbins;
        let valid_steps: Vec<_> = steps
            .clone()
            .into_iter()
            .filter(|s| *s > min_step_size)
            .collect();
        *valid_steps.get(0).unwrap_or_else(|| steps.last().unwrap())
    } else {
        // Otherwise, use span to determine the step size
        let level = (config.maxbins.ln() / logb).ceil();
        let minstep = config.minstep;
        let mut step = minstep.max(config.base.powf((span.ln() / logb).round() - level));

        // increase step size if too many bins
        while (span / step).ceil() > config.maxbins {
            step *= config.base;
        }

        // decrease step size if allowed
        for div in &config.divide {
            let v = step / div;
            if v >= minstep && span / v <= config.maxbins {
                step = v
            }
        }
        step
    };

    // Update precision of min_ and max_
    let v = step.ln();
    let precision = if v >= 0.0 {
        0.0
    } else {
        (-v / logb).floor() + 1.0
    };
    let eps = config.base.powf(-precision - 1.0);
    let (min_, max_) = if config.nice {
        let v = (min_ / step + eps).floor() * step;
        let min_ = if min_ < v { v - step } else { v };
        let max_ = (max_ / step).ceil() * step;
        (min_, max_)
    } else {
        (min_, max_)
    };

    // Compute start and stop
    let start = min_;
    let stop = if !approx_eq!(f64, max_, min_) {
        max_
    } else {
        min_ + step
    };

    // Handle anchor
    let (start, stop) = if let Some(anchor) = config.anchor {
        let shift = anchor - (start + step * ((anchor - start) / step).floor());
        (start + shift, stop + shift)
    } else {
        (start, stop)
    };

    Ok(BinParams {
        start,
        stop,
        step,
        n: ((stop - start) / step).ceil() as i32,
    })
}
