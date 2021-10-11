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
    #[prost(string, tag="2")]
    pub signal: ::prost::alloc::string::String,
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
    #[prost(string, tag="3")]
    pub signal: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub alias_0: ::prost::alloc::string::String,
    #[prost(string, tag="5")]
    pub alias_1: ::prost::alloc::string::String,
    /// A value in the binned domain at which to anchor the bins The bin boundaries will be shifted,
    /// if necessary, to ensure that a boundary aligns with the anchor value.
    #[prost(double, tag="6")]
    pub anchor: f64,
    /// The maximum number of bins allowed
    #[prost(double, tag="7")]
    pub maxbins: f64,
    /// The number base to use for automatic bin selection (e.g. base 10)
    #[prost(double, tag="8")]
    pub base: f64,
    /// An exact step size to use between bins. Overrides other options.
    #[prost(double, tag="9")]
    pub step: f64,
    /// A list of allowable step sizes to choose from
    #[prost(double, repeated, tag="10")]
    pub steps: ::prost::alloc::vec::Vec<f64>,
    /// The value span over which to generate bin boundaries. Defaults to the exact extent of the data
    #[prost(double, tag="11")]
    pub span: f64,
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
/// Top-level transform
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expression {
    #[prost(oneof="expression::Transform", tags="1, 2, 3, 4, 5, 6")]
    pub transform: ::core::option::Option<expression::Transform>,
}
/// Nested message and enum types in `Expression`.
pub mod expression {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Transform {
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
    }
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
    Variancp = 9,
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
