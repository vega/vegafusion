use crate::expression::compiler::compile;
use crate::expression::compiler::config::CompilationConfig;
use crate::expression::compiler::utils::ExprHelpers;
use crate::transform::TransformTrait;
use async_trait::async_trait;

use datafusion_expr::lit;

use datafusion_common::scalar::ScalarValue;
use datafusion_common::DFSchema;
use datafusion_expr::{abs, floor, when, Expr};
use float_cmp::approx_eq;
use std::ops::{Add, Div, Mul, Sub};
use std::sync::Arc;
use vegafusion_common::column::{flat_col, unescaped_col};
use vegafusion_common::data::scalar::ScalarValueHelpers;
use vegafusion_common::datatypes::to_numeric;
use vegafusion_core::arrow::datatypes::{DataType, Field};
use vegafusion_core::error::{Result, VegaFusionError};
use vegafusion_core::proto::gen::transforms::Bin;
use vegafusion_core::task_graph::task_value::TaskValue;
use vegafusion_dataframe::dataframe::DataFrame;

#[async_trait]
impl TransformTrait for Bin {
    async fn eval(
        &self,
        sql_df: Arc<dyn DataFrame>,
        config: &CompilationConfig,
    ) -> Result<(Arc<dyn DataFrame>, Vec<TaskValue>)> {
        let schema = sql_df.schema_df()?;

        // Compute binning solution
        let params = calculate_bin_params(self, &schema, config)?;

        let BinParams {
            start,
            stop,
            step,
            n,
        } = params;
        let bin_starts: Vec<f64> = (0..n).map(|i| start + step * i as f64).collect();
        let last_stop = *bin_starts.last().unwrap() + step;

        // Compute output signal value
        let output_value = compute_output_value(self, start, stop, step);

        let numeric_field = to_numeric(unescaped_col(&self.field), &sql_df.schema_df()?)?;

        // Add column with bin index
        let bin_index_name = "__bin_index";
        let bin_index =
            floor((numeric_field.clone().sub(lit(start)).div(lit(step))).add(lit(1.0e-14)))
                .alias(bin_index_name);
        let sql_df = sql_df.select(vec![Expr::Wildcard, bin_index]).await?;

        // Add column with bin start
        let bin_start = (flat_col(bin_index_name).mul(lit(step))).add(lit(start));
        let bin_start_name = self.alias_0.clone().unwrap_or_else(|| "bin0".to_string());

        let inf = lit(f64::INFINITY);
        let neg_inf = lit(f64::NEG_INFINITY);
        let eps = lit(1.0e-14);

        let bin_start = when(flat_col(bin_index_name).lt(lit(0.0)), neg_inf)
            .when(
                abs(numeric_field.sub(lit(last_stop)))
                    .lt(eps)
                    .and(flat_col(bin_index_name).eq(lit(n))),
                flat_col(bin_index_name)
                    .sub(lit(1))
                    .mul(lit(step))
                    .add(lit(start)),
            )
            .when(flat_col(bin_index_name).gt_eq(lit(n)), inf)
            .otherwise(bin_start)?
            .alias(&bin_start_name);

        let mut select_exprs = sql_df
            .schema()
            .fields
            .iter()
            .filter_map(|field| {
                if field.name() == &bin_start_name {
                    None
                } else {
                    Some(flat_col(field.name()))
                }
            })
            .collect::<Vec<_>>();
        select_exprs.push(bin_start);

        let sql_df = sql_df.select(select_exprs).await?;

        // Add bin end column
        let bin_end_name = self.alias_1.clone().unwrap_or_else(|| "bin1".to_string());
        let bin_end = (flat_col(&bin_start_name) + lit(step)).alias(&bin_end_name);

        // Compute final projection that removes __bin_index column
        let mut select_exprs = schema
            .fields()
            .iter()
            .filter_map(|field| {
                let name = field.name();
                if name == &bin_start_name || name == &bin_end_name {
                    None
                } else {
                    Some(flat_col(name))
                }
            })
            .collect::<Vec<_>>();
        select_exprs.push(flat_col(&bin_start_name));
        select_exprs.push(bin_end);

        let sql_df = sql_df.select(select_exprs).await?;

        Ok((sql_df, output_value.into_iter().collect()))
    }
}

fn compute_output_value(bin_tx: &Bin, start: f64, stop: f64, step: f64) -> Option<TaskValue> {
    let mut fname = bin_tx.field.clone();
    fname.insert_str(0, "bin_");

    let fields = ScalarValue::List(
        Some(vec![ScalarValue::from(bin_tx.field.as_str())]),
        Arc::new(Field::new("item", DataType::Utf8, true)),
    );

    if bin_tx.signal.is_some() {
        Some(TaskValue::Scalar(ScalarValue::from(vec![
            ("fields", fields),
            ("fname", ScalarValue::from(fname.as_str())),
            ("start", ScalarValue::from(start)),
            ("step", ScalarValue::from(step)),
            ("stop", ScalarValue::from(stop)),
        ])))
    } else {
        None
    }
}

#[derive(Clone, Debug)]
pub struct BinParams {
    pub start: f64,
    pub stop: f64,
    pub step: f64,
    pub n: i32,
}

pub fn calculate_bin_params(
    tx: &Bin,
    schema: &DFSchema,
    config: &CompilationConfig,
) -> Result<BinParams> {
    // Evaluate extent
    let extent_expr = compile(tx.extent.as_ref().unwrap(), config, Some(schema))?;
    let extent_scalar = extent_expr.eval_to_scalar()?;

    let extent = extent_scalar.to_f64x2().unwrap_or([0.0, 0.0]);

    let [min_, max_] = extent;
    if min_ > max_ {
        return Err(VegaFusionError::specification(format!(
            "extent[1] must be greater than extent[0]: Received {extent:?}"
        )));
    }

    // Initialize span to default value
    let mut span = if !approx_eq!(f64, min_, max_) {
        max_ - min_
    } else if !approx_eq!(f64, min_, 0.0) {
        min_.abs()
    } else {
        1.0
    };

    // Override span with specified value if available
    if let Some(span_expression) = &tx.span {
        let span_expr = compile(span_expression, config, Some(schema))?;
        let span_scalar = span_expr.eval_to_scalar()?;
        if let Ok(span_f64) = span_scalar.to_f64() {
            if span_f64 > 0.0 {
                span = span_f64;
            }
        }
    }

    let logb = tx.base.ln();

    let step = if let Some(step) = tx.step {
        // Use provided step as-is
        step
    } else if !tx.steps.is_empty() {
        // If steps is provided, limit step to one of the elements.
        // Choose the first element of steps that will result in fewer than maxmins
        let min_step_size = span / tx.maxbins;
        let valid_steps: Vec<_> = tx
            .steps
            .clone()
            .into_iter()
            .filter(|s| *s > min_step_size)
            .collect();
        *valid_steps
            .first()
            .unwrap_or_else(|| tx.steps.last().unwrap())
    } else {
        // Otherwise, use span to determine the step size
        let level = (tx.maxbins.ln() / logb).ceil();
        let minstep = tx.minstep;
        let mut step = minstep.max(tx.base.powf((span.ln() / logb).round() - level));

        // increase step size if too many bins
        while (span / step).ceil() > tx.maxbins {
            step *= tx.base;
        }

        // decrease step size if allowed
        for div in &tx.divide {
            let v = step / div;
            if v >= minstep && span / v <= tx.maxbins {
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
    let eps = tx.base.powf(-precision - 1.0);
    let (min_, max_) = if tx.nice {
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
    let (start, stop) = if let Some(anchor) = tx.anchor {
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
