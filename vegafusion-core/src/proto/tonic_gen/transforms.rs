/// Filter
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Filter {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<super::expression::Expression>,
}
/// Formula
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Formula {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<super::expression::Expression>,
    #[prost(string, tag = "2")]
    pub r#as: ::prost::alloc::string::String,
}
/// Extent
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Extent {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "2")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
}
/// Collect
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Collect {
    #[prost(string, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration = "SortOrder", repeated, tag = "2")]
    pub order: ::prost::alloc::vec::Vec<i32>,
}
/// Bin
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Bin {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub extent: ::core::option::Option<super::expression::Expression>,
    #[prost(string, optional, tag = "3")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "4")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "5")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
    /// A value in the binned domain at which to anchor the bins The bin boundaries will be shifted,
    /// if necessary, to ensure that a boundary aligns with the anchor value.
    #[prost(double, optional, tag = "6")]
    pub anchor: ::core::option::Option<f64>,
    /// The maximum number of bins allowed
    #[prost(double, tag = "7")]
    pub maxbins: f64,
    /// The number base to use for automatic bin selection (e.g. base 10)
    #[prost(double, tag = "8")]
    pub base: f64,
    /// An exact step size to use between bins. Overrides other options.
    #[prost(double, optional, tag = "9")]
    pub step: ::core::option::Option<f64>,
    /// A list of allowable step sizes to choose from
    #[prost(double, repeated, tag = "10")]
    pub steps: ::prost::alloc::vec::Vec<f64>,
    /// The value span over which to generate bin boundaries. Defaults to the exact extent of the data
    #[prost(message, optional, tag = "11")]
    pub span: ::core::option::Option<super::expression::Expression>,
    /// A minimum distance between adjacent bins
    #[prost(double, tag = "12")]
    pub minstep: f64,
    /// Scale factors indicating the allowed subdivisions. The defualt value is vec!\[5.0, 2.0\],
    /// which indicates that for base 10 numbers, the method may consider dividing bin sizes by 5 and/or 2.
    #[prost(double, repeated, tag = "13")]
    pub divide: ::prost::alloc::vec::Vec<f64>,
    /// If true, attempt to make the bin boundaries use human-friendly boundaries
    /// (e.g. whole numbers, multiples of 10, etc.)
    #[prost(bool, tag = "14")]
    pub nice: bool,
}
/// Aggregate
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Aggregate {
    #[prost(string, repeated, tag = "1")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "2")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration = "AggregateOp", repeated, tag = "4")]
    pub ops: ::prost::alloc::vec::Vec<i32>,
}
/// JoinAggregate
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinAggregate {
    #[prost(string, repeated, tag = "1")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "2")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(enumeration = "AggregateOp", repeated, tag = "3")]
    pub ops: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "4")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// TimeUnit
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TimeUnit {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration = "TimeUnitUnit", repeated, tag = "2")]
    pub units: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, optional, tag = "3")]
    pub signal: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "4")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "5")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(enumeration = "TimeUnitTimeZone", optional, tag = "6")]
    pub timezone: ::core::option::Option<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowTransformOp {
    #[prost(oneof = "window_transform_op::Op", tags = "1, 2")]
    pub op: ::core::option::Option<window_transform_op::Op>,
}
/// Nested message and enum types in `WindowTransformOp`.
pub mod window_transform_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Op {
        #[prost(enumeration = "super::AggregateOp", tag = "1")]
        AggregateOp(i32),
        #[prost(enumeration = "super::WindowOp", tag = "2")]
        WindowOp(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Window {
    #[prost(enumeration = "SortOrder", repeated, tag = "1")]
    pub sort: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "2")]
    pub sort_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "3")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "4")]
    pub ops: ::prost::alloc::vec::Vec<WindowTransformOp>,
    #[prost(string, repeated, tag = "5")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(double, repeated, tag = "6")]
    pub params: ::prost::alloc::vec::Vec<f64>,
    #[prost(string, repeated, tag = "7")]
    pub aliases: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "8")]
    pub frame: ::core::option::Option<WindowFrame>,
    #[prost(bool, optional, tag = "9")]
    pub ignore_peers: ::core::option::Option<bool>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrame {
    #[prost(int64, optional, tag = "1")]
    pub start: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "2")]
    pub end: ::core::option::Option<i64>,
}
/// Project
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Project {
    #[prost(string, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Stack
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Stack {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(enumeration = "StackOffset", tag = "2")]
    pub offset: i32,
    #[prost(enumeration = "SortOrder", repeated, tag = "3")]
    pub sort: ::prost::alloc::vec::Vec<i32>,
    #[prost(string, repeated, tag = "4")]
    pub sort_fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "5")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "6")]
    pub alias_0: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "7")]
    pub alias_1: ::core::option::Option<::prost::alloc::string::String>,
}
/// Impute
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Impute {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub key: ::prost::alloc::string::String,
    #[prost(enumeration = "ImputeMethod", tag = "3")]
    pub method: i32,
    #[prost(string, repeated, tag = "4")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "5")]
    pub value_json: ::core::option::Option<::prost::alloc::string::String>,
}
/// Pivot
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Pivot {
    #[prost(string, tag = "1")]
    pub field: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub groupby: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(int32, optional, tag = "4")]
    pub limit: ::core::option::Option<i32>,
    #[prost(enumeration = "AggregateOp", optional, tag = "5")]
    pub op: ::core::option::Option<i32>,
}
/// Identifier
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Identifier {
    #[prost(string, tag = "1")]
    pub r#as: ::prost::alloc::string::String,
}
/// Fold
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fold {
    #[prost(string, repeated, tag = "1")]
    pub fields: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "2")]
    pub r#as: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Sequence
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Sequence {
    #[prost(message, optional, tag = "1")]
    pub start: ::core::option::Option<super::expression::Expression>,
    #[prost(message, optional, tag = "2")]
    pub stop: ::core::option::Option<super::expression::Expression>,
    #[prost(message, optional, tag = "3")]
    pub step: ::core::option::Option<super::expression::Expression>,
    #[prost(string, optional, tag = "4")]
    pub r#as: ::core::option::Option<::prost::alloc::string::String>,
}
/// Top-level transform
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transform {
    #[prost(
        oneof = "transform::TransformKind",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
    )]
    pub transform_kind: ::core::option::Option<transform::TransformKind>,
}
/// Nested message and enum types in `Transform`.
pub mod transform {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TransformKind {
        #[prost(message, tag = "1")]
        Filter(super::Filter),
        #[prost(message, tag = "2")]
        Extent(super::Extent),
        #[prost(message, tag = "3")]
        Formula(super::Formula),
        #[prost(message, tag = "4")]
        Bin(super::Bin),
        #[prost(message, tag = "5")]
        Aggregate(super::Aggregate),
        #[prost(message, tag = "6")]
        Collect(super::Collect),
        #[prost(message, tag = "7")]
        Timeunit(super::TimeUnit),
        #[prost(message, tag = "8")]
        Joinaggregate(super::JoinAggregate),
        #[prost(message, tag = "9")]
        Window(super::Window),
        #[prost(message, tag = "10")]
        Project(super::Project),
        #[prost(message, tag = "11")]
        Stack(super::Stack),
        #[prost(message, tag = "12")]
        Impute(super::Impute),
        #[prost(message, tag = "13")]
        Pivot(super::Pivot),
        #[prost(message, tag = "14")]
        Identifier(super::Identifier),
        #[prost(message, tag = "15")]
        Fold(super::Fold),
        #[prost(message, tag = "16")]
        Sequence(super::Sequence),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformPipeline {
    #[prost(message, repeated, tag = "1")]
    pub transforms: ::prost::alloc::vec::Vec<Transform>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SortOrder {
    Descending = 0,
    Ascending = 1,
}
impl SortOrder {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            SortOrder::Descending => "Descending",
            SortOrder::Ascending => "Ascending",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Descending" => Some(Self::Descending),
            "Ascending" => Some(Self::Ascending),
            _ => None,
        }
    }
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
impl AggregateOp {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregateOp::Count => "Count",
            AggregateOp::Valid => "Valid",
            AggregateOp::Missing => "Missing",
            AggregateOp::Distinct => "Distinct",
            AggregateOp::Sum => "Sum",
            AggregateOp::Product => "Product",
            AggregateOp::Mean => "Mean",
            AggregateOp::Average => "Average",
            AggregateOp::Variance => "Variance",
            AggregateOp::Variancep => "Variancep",
            AggregateOp::Stdev => "Stdev",
            AggregateOp::Stdevp => "Stdevp",
            AggregateOp::Stderr => "Stderr",
            AggregateOp::Median => "Median",
            AggregateOp::Q1 => "Q1",
            AggregateOp::Q3 => "Q3",
            AggregateOp::Ci0 => "Ci0",
            AggregateOp::Ci1 => "Ci1",
            AggregateOp::Min => "Min",
            AggregateOp::Max => "Max",
            AggregateOp::Argmin => "Argmin",
            AggregateOp::Argmax => "Argmax",
            AggregateOp::Values => "Values",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Count" => Some(Self::Count),
            "Valid" => Some(Self::Valid),
            "Missing" => Some(Self::Missing),
            "Distinct" => Some(Self::Distinct),
            "Sum" => Some(Self::Sum),
            "Product" => Some(Self::Product),
            "Mean" => Some(Self::Mean),
            "Average" => Some(Self::Average),
            "Variance" => Some(Self::Variance),
            "Variancep" => Some(Self::Variancep),
            "Stdev" => Some(Self::Stdev),
            "Stdevp" => Some(Self::Stdevp),
            "Stderr" => Some(Self::Stderr),
            "Median" => Some(Self::Median),
            "Q1" => Some(Self::Q1),
            "Q3" => Some(Self::Q3),
            "Ci0" => Some(Self::Ci0),
            "Ci1" => Some(Self::Ci1),
            "Min" => Some(Self::Min),
            "Max" => Some(Self::Max),
            "Argmin" => Some(Self::Argmin),
            "Argmax" => Some(Self::Argmax),
            "Values" => Some(Self::Values),
            _ => None,
        }
    }
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
impl TimeUnitUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnitUnit::Year => "Year",
            TimeUnitUnit::Quarter => "Quarter",
            TimeUnitUnit::Month => "Month",
            TimeUnitUnit::Date => "Date",
            TimeUnitUnit::Week => "Week",
            TimeUnitUnit::Day => "Day",
            TimeUnitUnit::DayOfYear => "DayOfYear",
            TimeUnitUnit::Hours => "Hours",
            TimeUnitUnit::Minutes => "Minutes",
            TimeUnitUnit::Seconds => "Seconds",
            TimeUnitUnit::Milliseconds => "Milliseconds",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Year" => Some(Self::Year),
            "Quarter" => Some(Self::Quarter),
            "Month" => Some(Self::Month),
            "Date" => Some(Self::Date),
            "Week" => Some(Self::Week),
            "Day" => Some(Self::Day),
            "DayOfYear" => Some(Self::DayOfYear),
            "Hours" => Some(Self::Hours),
            "Minutes" => Some(Self::Minutes),
            "Seconds" => Some(Self::Seconds),
            "Milliseconds" => Some(Self::Milliseconds),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnitTimeZone {
    Local = 0,
    Utc = 1,
}
impl TimeUnitTimeZone {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnitTimeZone::Local => "Local",
            TimeUnitTimeZone::Utc => "Utc",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Local" => Some(Self::Local),
            "Utc" => Some(Self::Utc),
            _ => None,
        }
    }
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
impl WindowOp {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WindowOp::RowNumber => "RowNumber",
            WindowOp::Rank => "Rank",
            WindowOp::DenseRank => "DenseRank",
            WindowOp::PercentileRank => "PercentileRank",
            WindowOp::CumeDist => "CumeDist",
            WindowOp::NTile => "NTile",
            WindowOp::Lag => "Lag",
            WindowOp::Lead => "Lead",
            WindowOp::FirstValue => "FirstValue",
            WindowOp::LastValue => "LastValue",
            WindowOp::NthValue => "NthValue",
            WindowOp::PrevValue => "PrevValue",
            WindowOp::NextValue => "NextValue",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "RowNumber" => Some(Self::RowNumber),
            "Rank" => Some(Self::Rank),
            "DenseRank" => Some(Self::DenseRank),
            "PercentileRank" => Some(Self::PercentileRank),
            "CumeDist" => Some(Self::CumeDist),
            "NTile" => Some(Self::NTile),
            "Lag" => Some(Self::Lag),
            "Lead" => Some(Self::Lead),
            "FirstValue" => Some(Self::FirstValue),
            "LastValue" => Some(Self::LastValue),
            "NthValue" => Some(Self::NthValue),
            "PrevValue" => Some(Self::PrevValue),
            "NextValue" => Some(Self::NextValue),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StackOffset {
    Zero = 0,
    Center = 1,
    Normalize = 2,
}
impl StackOffset {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StackOffset::Zero => "Zero",
            StackOffset::Center => "Center",
            StackOffset::Normalize => "Normalize",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Zero" => Some(Self::Zero),
            "Center" => Some(Self::Center),
            "Normalize" => Some(Self::Normalize),
            _ => None,
        }
    }
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
impl ImputeMethod {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ImputeMethod::ImputeValue => "ImputeValue",
            ImputeMethod::ImputeMean => "ImputeMean",
            ImputeMethod::ImputeMedian => "ImputeMedian",
            ImputeMethod::ImputeMax => "ImputeMax",
            ImputeMethod::ImputeMin => "ImputeMin",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ImputeValue" => Some(Self::ImputeValue),
            "ImputeMean" => Some(Self::ImputeMean),
            "ImputeMedian" => Some(Self::ImputeMedian),
            "ImputeMax" => Some(Self::ImputeMax),
            "ImputeMin" => Some(Self::ImputeMin),
            _ => None,
        }
    }
}
