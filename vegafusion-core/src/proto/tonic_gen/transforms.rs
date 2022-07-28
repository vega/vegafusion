/// Filter
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(message, optional, tag="1")]
    pub expr: ::core::option::Option<super::expression::Expression>,
}
/// Formula
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Formula {
    #[prost(message, optional, tag="1")]
    pub expr: ::core::option::Option<super::expression::Expression>,
    #[prost(string, tag="2")]
    pub r#as: ::prost::alloc::string::String,
}
/// Extent
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extent {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, optional, tag="2")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
}
/// Collect
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Collect {
    #[prost(string, repeated, tag="1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration="SortOrder", repeated, tag="2")]
    pub order: ::prost::alloc::vec::Vec<i32>,
}
/// Bin
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Bin {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub extent: ::core::option::Option<super::expression::Expression>,
    #[prost(string, optional, tag="3")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="5")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
    /// A value in the binned domain at which to anchor the bins The bin boundaries will be shifted,
    /// if necessary, to ensure that a boundary aligns with the anchor value.
    #[prost(double, optional, tag="6")]
    pub anchor: ::core::option::Option<f64>,
    /// The maximum number of bins allowed
    #[prost(double, tag="7")]
    pub maxbins: f64,
    /// The number base to use for automatic bin selection (e.g. base 10)
    #[prost(double, tag="8")]
    pub base: f64,
    /// An exact step size to use between bins. Overrides other options.
    #[prost(double, optional, tag="9")]
    pub step: ::core::option::Option<f64>,
    /// A list of allowable step sizes to choose from
    #[prost(double, repeated, tag="10")]
    pub steps: ::prost::alloc::vec::Vec<f64>,
    /// The value span over which to generate bin boundaries. Defaults to the exact extent of the data
    #[prost(message, optional, tag="11")]
    pub span: ::core::option::Option<super::expression::Expression>,
    /// A minimum distance between adjacent bins
    #[prost(double, tag="12")]
    pub minstep: f64,
    /// Scale factors indicating the allowed subdivisions. The defualt value is vec![5.0, 2.0],
    /// which indicates that for base 10 numbers, the method may consider dividing bin sizes by 5 and/or 2.
    #[prost(double, repeated, tag="13")]
    pub divide: ::prost::alloc::vec::Vec<f64>,
    /// If true, attempt to make the bin boundaries use human-friendly boundaries
    /// (e.g. whole numbers, multiples of 10, etc.)
    #[prost(bool, tag="14")]
    pub nice: bool,
}
/// Aggregate
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Aggregate {
    #[prost(string, repeated, tag="1")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="2")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration="AggregateOp", repeated, tag="4")]
    pub ops: ::prost::alloc::vec::Vec<i32>,
}
/// JoinAggregate
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinAggregate {
    #[prost(string, repeated, tag="1")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="2")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration="AggregateOp", repeated, tag="3")]
    pub ops: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag="4")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// TimeUnit
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeUnit {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="TimeUnitUnit", repeated, tag="2")]
    pub units: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, optional, tag="3")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="5")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(enumeration="TimeUnitTimeZone", optional, tag="6")]
    pub timezone: ::core::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowTransformOp {
    #[prost(oneof="window_transform_op::Op", tags="1, 2")]
    pub op: ::core::option::Option<window_transform_op::Op>,
}
/// Nested message and enum types in `WindowTransformOp`.
pub mod window_transform_op {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Op {
        #[prost(enumeration="super::AggregateOp", tag="1")]
        AggregateOp(i32),
        #[prost(enumeration="super::WindowOp", tag="2")]
        WindowOp(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Window {
    #[prost(enumeration="SortOrder", repeated, tag="1")]
    pub sort: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag="2")]
    pub sort_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="3")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag="4")]
    pub ops: ::prost::alloc::vec::Vec<WindowTransformOp>,
    #[prost(string, repeated, tag="5")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(double, repeated, tag="6")]
    pub params: ::prost::alloc::vec::Vec<f64>,
    #[prost(string, repeated, tag="7")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag="8")]
    pub frame: ::core::option::Option<WindowFrame>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrame {
    #[prost(int64, optional, tag="1")]
    pub start: ::core::option::Option<i64>,
    #[prost(int64, optional, tag="2")]
    pub end: ::core::option::Option<i64>,
}
/// Project
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Project {
    #[prost(string, repeated, tag="1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Stack
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stack {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration="StackOffset", tag="2")]
    pub offset: i32,
    #[prost(enumeration="SortOrder", repeated, tag="3")]
    pub sort: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag="4")]
    pub sort_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="5")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag="6")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="7")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
}
/// Impute
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Impute {
    #[prost(string, tag="1")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub key: ::prost::alloc::string::String,
    #[prost(enumeration="ImputeMethod", tag="3")]
    pub method: i32,
    #[prost(string, repeated, tag="4")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag="5")]
    pub value_json: ::core::option::Option<::prost::alloc::string::String>,
}
/// Top-level transform
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transform {
    #[prost(oneof="transform::TransformKind", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12")]
    pub transform_kind: ::core::option::Option<transform::TransformKind>,
}
/// Nested message and enum types in `Transform`.
pub mod transform {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TransformKind {
        #[prost(message, tag="1")]
        Filter(super::Filter),
        #[prost(message, tag="2")]
        Extent(super::Extent),
        #[prost(message, tag="3")]
        Formula(super::Formula),
        #[prost(message, tag="4")]
        Bin(super::Bin),
        #[prost(message, tag="5")]
        Aggregate(super::Aggregate),
        #[prost(message, tag="6")]
        Collect(super::Collect),
        #[prost(message, tag="7")]
        Timeunit(super::TimeUnit),
        #[prost(message, tag="8")]
        Joinaggregate(super::JoinAggregate),
        #[prost(message, tag="9")]
        Window(super::Window),
        #[prost(message, tag="10")]
        Project(super::Project),
        #[prost(message, tag="11")]
        Stack(super::Stack),
        #[prost(message, tag="12")]
        Impute(super::Impute),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformPipeline {
    #[prost(message, repeated, tag="1")]
    pub transforms: ::prost::alloc::vec::Vec<Transform>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SortOrder {
    Descending = 0,
    Ascending = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateOp {
    Count = 0,
    Valid = 1,
    Missing = 2,
    Distinct = 3,
    Sum = 4,
    Product = 5,
    Mean = 6,
    Average = 7,
    Variance = 8,
    Variancep = 9,
    Stdev = 10,
    Stdevp = 11,
    Stderr = 12,
    Median = 13,
    Q1 = 14,
    Q3 = 15,
    Ci0 = 16,
    Ci1 = 17,
    Min = 18,
    Max = 19,
    Argmin = 20,
    Argmax = 21,
    Values = 22,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnitUnit {
    Year = 0,
    Quarter = 1,
    Month = 2,
    Date = 3,
    Week = 4,
    Day = 5,
    DayOfYear = 6,
    Hours = 7,
    Minutes = 8,
    Seconds = 9,
    Milliseconds = 10,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnitTimeZone {
    Local = 0,
    Utc = 1,
}
/// Window
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WindowOp {
    RowNumber = 0,
    Rank = 1,
    DenseRank = 2,
    PercentileRank = 3,
    CumeDist = 4,
    NTile = 5,
    Lag = 6,
    Lead = 7,
    FirstValue = 8,
    LastValue = 9,
    NthValue = 10,
    PrevValue = 11,
    NextValue = 12,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StackOffset {
    Zero = 0,
    Center = 1,
    Normalize = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ImputeMethod {
    ImputeValue = 0,
    ImputeMean = 1,
    ImputeMedian = 2,
    ImputeMax = 3,
    ImputeMin = 4,
}
