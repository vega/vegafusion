/*
 * VegaFusion
 * Copyright (C) 2022 VegaFusion Technologies LLC
 *
 * This program is distributed under multiple licenses.
 * Please consult the license documentation provided alongside
 * this program the details of the active license.
 */
use crate::error::Result;
use crate::expression::parser::parse;
use crate::proto::gen::expression::expression::Expr;
use crate::proto::gen::expression::{ArrayExpression, Expression, Literal};
use crate::proto::gen::tasks::Variable;
use crate::proto::gen::transforms::Bin;
use crate::spec::transform::bin::{BinExtent, BinSpan, BinTransformSpec};
use crate::spec::values::SignalExpressionSpec;
use crate::task_graph::task::InputVariable;
use crate::transform::TransformDependencies;

impl Bin {
    pub fn try_new(transform: &BinTransformSpec) -> Result<Self> {
        let field = transform.field.field();

        let extent_expr = match &transform.extent {
            BinExtent::Value(extent) => {
                // Convert extent value to an expression for consistency
                Expression::new(
                    Expr::from(ArrayExpression::new(vec![
                        Expression::new(
                            Expr::from(Literal::new(extent[0], &extent[0].to_string())),
                            None,
                        ),
                        Expression::new(
                            Expr::from(Literal::new(extent[1], &extent[1].to_string())),
                            None,
                        ),
                    ])),
                    None,
                )
            }
            BinExtent::Signal(SignalExpressionSpec { signal }) => parse(signal)?,
        };

        let config = BinConfig::from_spec(transform.clone())?;
        let as_ = transform.as_.clone().unwrap_or_default();

        Ok(Self {
            field,
            extent: Some(extent_expr),
            alias_0: as_.get(0).cloned(),
            alias_1: as_.get(1).cloned(),
            anchor: config.anchor,
            maxbins: config.maxbins,
            base: config.base,
            step: config.step,
            steps: config.steps.into_iter().flatten().collect(),
            span: config.span,
            minstep: config.minstep,
            divide: config.divide,
            signal: transform.signal.clone(),
            nice: config.nice,
        })
    }
}

// Port of https://github.com/vega/vega/blob/v5.9.1/packages/vega-statistics/src/bin.js
// with credit to
// https://github.com/altair-viz/altair-transform/blob/master/altair_transform/transform/vega_utils.py
#[derive(Debug, Clone)]
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
    span: Option<Expression>,
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
    pub fn from_spec(spec: BinTransformSpec) -> Result<Self> {
        let dflt = Self::default();

        let span = match &spec.span {
            None => None,
            Some(span) => match span {
                BinSpan::Value(span) => Some(Expression::from(*span)),
                BinSpan::Signal(signal) => Some(parse(&signal.signal)?),
            },
        };

        Ok(Self {
            anchor: spec.anchor,
            base: spec.base.unwrap_or(dflt.base),
            divide: spec.divide.unwrap_or(dflt.divide),
            maxbins: spec.maxbins.unwrap_or(dflt.maxbins),
            minstep: spec.minstep.unwrap_or(dflt.minstep),
            nice: spec.nice.unwrap_or(dflt.nice),
            step: spec.step,
            steps: spec.steps,
            span,
        })
    }
}

impl TransformDependencies for Bin {
    fn input_vars(&self) -> Vec<InputVariable> {
        let mut input_vars = self.extent.as_ref().unwrap().input_vars();
        if let Some(span) = self.span.as_ref() {
            input_vars.extend(span.input_vars());
        }
        input_vars
    }

    fn output_vars(&self) -> Vec<Variable> {
        self.signal
            .iter()
            .map(|s| Variable::new_signal(s))
            .collect()
    }
}
