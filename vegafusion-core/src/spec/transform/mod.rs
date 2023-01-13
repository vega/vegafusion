pub mod aggregate;
pub mod bin;
pub mod collect;
pub mod extent;
pub mod filter;
pub mod fold;
pub mod formula;
pub mod identifier;
pub mod impute;
pub mod joinaggregate;
pub mod lookup;
pub mod pivot;
pub mod project;
pub mod sequence;
pub mod stack;
pub mod timeunit;
pub mod unsupported;
pub mod window;

use crate::spec::transform::{extent::ExtentTransformSpec, filter::FilterTransformSpec};

use crate::error::Result;
use crate::expression::column_usage::{ColumnUsage, DatasetsColumnUsage, VlSelectionFields};
use crate::planning::plan::PlannerConfig;
use crate::spec::transform::aggregate::AggregateTransformSpec;
use crate::spec::transform::bin::BinTransformSpec;
use crate::spec::transform::collect::CollectTransformSpec;
use crate::spec::transform::fold::FoldTransformSpec;
use crate::spec::transform::formula::FormulaTransformSpec;
use crate::spec::transform::identifier::IdentifierTransformSpec;
use crate::spec::transform::impute::ImputeTransformSpec;
use crate::spec::transform::joinaggregate::JoinAggregateTransformSpec;
use crate::spec::transform::lookup::LookupTransformSpec;
use crate::spec::transform::pivot::PivotTransformSpec;
use crate::spec::transform::project::ProjectTransformSpec;
use crate::spec::transform::sequence::SequenceTransformSpec;
use crate::spec::transform::stack::StackTransformSpec;
use crate::spec::transform::timeunit::TimeUnitTransformSpec;
use crate::spec::transform::unsupported::*;
use crate::spec::transform::window::WindowTransformSpec;
use crate::task_graph::graph::ScopedVariable;
use crate::task_graph::scope::TaskScope;
use crate::task_graph::task::InputVariable;
use serde::{Deserialize, Serialize};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TransformSpec {
    Extent(ExtentTransformSpec),
    Filter(FilterTransformSpec),
    Formula(FormulaTransformSpec),
    Bin(Box<BinTransformSpec>), // Box since transform is much larger than others
    Aggregate(AggregateTransformSpec),
    Collect(CollectTransformSpec),
    Timeunit(TimeUnitTransformSpec),
    JoinAggregate(JoinAggregateTransformSpec),
    Window(WindowTransformSpec),
    Project(ProjectTransformSpec),
    Stack(StackTransformSpec),
    Impute(ImputeTransformSpec),
    Pivot(PivotTransformSpec),
    Identifier(IdentifierTransformSpec),
    Fold(FoldTransformSpec),
    Sequence(SequenceTransformSpec),

    // Unsupported
    CountPattern(CountpatternTransformSpec),
    Contour(ContourTransformSpec),
    Cross(CrossTransformSpec),
    Crossfilter(CrossfilterTransformSpec),
    Density(DensityTransformSpec),
    DotBin(DotbinTransformSpec),
    Flatten(FlattenTransformSpec),
    Force(ForceTransformSpec),
    GeoJson(GeojsonTransformSpec),
    GeoPath(GeopathTransformSpec),
    GeoPoint(GeopointTransformSpec),
    GeoShape(GeoshapeTransformSpec),
    Graticule(GraticuleTransformSpec),
    Heatmap(HeatmapTransformSpec),
    IsoContour(IsocontourTransformSpec),
    Kde(KdeTransformSpec),
    Kde2d(Kde2dTransformSpec),
    Label(LabelTransformSpec),
    LinkPath(LinkpathTransformSpec),
    Loess(LoessTransformSpec),
    Lookup(LookupTransformSpec),
    Nest(NestTransformSpec),
    Pack(PackTransformSpec),
    Partition(PartitionTransformSpec),
    Pie(PieTransformSpec),
    Quantile(QuantileTransformSpec),
    Regression(RegressionTransformSpec),
    ResolveFilter(ResolvefilterTransformSpec),
    Sample(SampleTransformSpec),
    Stratify(StratifyTransformSpec),
    Tree(TreeTransformSpec),
    TreeLinks(TreelinksTransformSpec),
    Treemap(TreemapTransformSpec),
    Voronoi(VoronoiTransformSpec),
    WordCloud(WordcloudTransformSpec),
}

impl Deref for TransformSpec {
    type Target = dyn TransformSpecTrait;

    fn deref(&self) -> &Self::Target {
        match self {
            TransformSpec::Extent(t) => t,
            TransformSpec::Filter(t) => t,
            TransformSpec::Formula(t) => t,
            TransformSpec::Bin(t) => t.as_ref(),
            TransformSpec::Aggregate(t) => t,
            TransformSpec::Collect(t) => t,
            TransformSpec::Timeunit(t) => t,
            TransformSpec::Project(t) => t,
            TransformSpec::Stack(t) => t,
            TransformSpec::Impute(t) => t,
            TransformSpec::Pivot(t) => t,
            TransformSpec::Sequence(t) => t,

            // Supported for dependency determination, not implementation
            TransformSpec::Lookup(t) => t,

            // Unsupported
            TransformSpec::CountPattern(t) => t,
            TransformSpec::Contour(t) => t,
            TransformSpec::Cross(t) => t,
            TransformSpec::Crossfilter(t) => t,
            TransformSpec::Density(t) => t,
            TransformSpec::DotBin(t) => t,
            TransformSpec::Flatten(t) => t,
            TransformSpec::Fold(t) => t,
            TransformSpec::Force(t) => t,
            TransformSpec::GeoJson(t) => t,
            TransformSpec::GeoPath(t) => t,
            TransformSpec::GeoPoint(t) => t,
            TransformSpec::GeoShape(t) => t,
            TransformSpec::Graticule(t) => t,
            TransformSpec::Heatmap(t) => t,
            TransformSpec::Identifier(t) => t,
            TransformSpec::IsoContour(t) => t,
            TransformSpec::JoinAggregate(t) => t,
            TransformSpec::Kde(t) => t,
            TransformSpec::Kde2d(t) => t,
            TransformSpec::Label(t) => t,
            TransformSpec::LinkPath(t) => t,
            TransformSpec::Loess(t) => t,
            TransformSpec::Nest(t) => t,
            TransformSpec::Pack(t) => t,
            TransformSpec::Partition(t) => t,
            TransformSpec::Pie(t) => t,
            TransformSpec::Quantile(t) => t,
            TransformSpec::Regression(t) => t,
            TransformSpec::ResolveFilter(t) => t,
            TransformSpec::Sample(t) => t,
            TransformSpec::Stratify(t) => t,
            TransformSpec::Tree(t) => t,
            TransformSpec::TreeLinks(t) => t,
            TransformSpec::Treemap(t) => t,
            TransformSpec::Voronoi(t) => t,
            TransformSpec::Window(t) => t,
            TransformSpec::WordCloud(t) => t,
        }
    }
}

pub trait TransformSpecTrait {
    fn supported(&self) -> bool {
        true
    }

    fn supported_and_allowed(
        &self,
        planner_config: &PlannerConfig,
        task_scope: &TaskScope,
        scope: &[u32],
    ) -> bool {
        let input_vars = self.input_vars().unwrap_or_default();
        for input_var in &input_vars {
            if let Ok(resolved) = task_scope.resolve_scope(&input_var.var, scope) {
                let scoped_var: ScopedVariable = (resolved.var, resolved.scope);
                if planner_config.client_only_vars.contains(&scoped_var) {
                    // Transform requires a variable that may only live on the client
                    return false;
                }
            }
        }
        self.supported()
    }

    fn output_signals(&self) -> Vec<String> {
        Default::default()
    }

    fn input_vars(&self) -> Result<Vec<InputVariable>> {
        Ok(Default::default())
    }

    fn transform_columns(
        &self,
        _datum_var: &Option<ScopedVariable>,
        _usage_scope: &[u32],
        _task_scope: &TaskScope,
        _vl_selection_fields: &VlSelectionFields,
    ) -> TransformColumns {
        TransformColumns::Unknown
    }

    fn local_datetime_columns_produced(
        &self,
        _input_local_datetime_columns: &[String],
    ) -> Vec<String> {
        Vec::new()
    }
}

pub enum TransformColumns {
    /// Transforms that pass through existing columns and produce zero or more new columns
    PassThrough {
        usage: DatasetsColumnUsage,
        produced: ColumnUsage,
    },

    /// Transforms that overwrite all input columns, leaving only those produced by the transform
    Overwrite {
        usage: DatasetsColumnUsage,
        produced: ColumnUsage,
    },

    /// Transforms with unknown usage and/or production of columns
    Unknown,
}
