/// ## Task Value
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskValue {
    #[prost(oneof="task_value::Data", tags="1, 2")]
    pub data: ::core::option::Option<task_value::Data>,
}
/// Nested message and enum types in `TaskValue`.
pub mod task_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Data {
        ///
        /// Representation of scalar as single column, single row, record batch in Arrow IPC format
        #[prost(bytes, tag="1")]
        Scalar(::prost::alloc::vec::Vec<u8>),
        ///
        /// Serialized Arrow record batch in Arrow IPC format
        #[prost(bytes, tag="2")]
        Table(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScopedVariable {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration="VariableNamespace", tag="2")]
    pub namespace: i32,
    #[prost(uint32, repeated, tag="3")]
    pub scope: ::prost::alloc::vec::Vec<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Variable {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration="VariableNamespace", tag="2")]
    pub namespace: i32,
}
/// ## Scan URL Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParseFieldSpec {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub datatype: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParseFieldSpecs {
    #[prost(message, repeated, tag="1")]
    pub specs: ::prost::alloc::vec::Vec<ParseFieldSpec>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScanUrlFormat {
    ///
    /// The data format type. The currently supported data formats are json (the default),
    /// csv (comma-separated values), tsv (tab-separated values), dsv (delimited text files),
    /// and topojson.
    #[prost(string, tag="1")]
    pub r#type: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub property: ::prost::alloc::string::String,
    #[prost(string, repeated, tag="5")]
    pub header: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag="6")]
    pub delimiter: ::prost::alloc::string::String,
    #[prost(string, tag="7")]
    pub feature: ::prost::alloc::string::String,
    ///
    /// JSON encoded string:
    /// If set to auto, perform automatic type inference to determine the desired data types.
    /// Alternatively, a parsing directive object can be provided for explicit data types.
    /// Each property of the object corresponds to a field name, and the value to the desired data type
    /// (one of "boolean", "date", "number" or "string"). For example, "parse": {"modified_on": "date"}
    /// parses the modified_on field in each input record as a Date value. Specific date formats can
    /// be provided (e.g., {"foo": "date:'%m%d%Y'"}), using the d3-time-format syntax. UTC date format
    /// parsing is supported similarly (e.g., {"foo": "utc:'%m%d%Y'"}).
    #[prost(oneof="scan_url_format::Parse", tags="2, 3")]
    pub parse: ::core::option::Option<scan_url_format::Parse>,
}
/// Nested message and enum types in `ScanUrlFormat`.
pub mod scan_url_format {
    ///
    /// JSON encoded string:
    /// If set to auto, perform automatic type inference to determine the desired data types.
    /// Alternatively, a parsing directive object can be provided for explicit data types.
    /// Each property of the object corresponds to a field name, and the value to the desired data type
    /// (one of "boolean", "date", "number" or "string"). For example, "parse": {"modified_on": "date"}
    /// parses the modified_on field in each input record as a Date value. Specific date formats can
    /// be provided (e.g., {"foo": "date:'%m%d%Y'"}), using the d3-time-format syntax. UTC date format
    /// parsing is supported similarly (e.g., {"foo": "utc:'%m%d%Y'"}).
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Parse {
        #[prost(string, tag="2")]
        String(::prost::alloc::string::String),
        #[prost(message, tag="3")]
        Object(super::ParseFieldSpecs),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScanUrlTask {
    #[prost(message, optional, tag="1")]
    pub url: ::core::option::Option<Variable>,
    #[prost(int32, tag="2")]
    pub batch_size: i32,
    #[prost(message, optional, tag="3")]
    pub format_type: ::core::option::Option<ScanUrlFormat>,
}
/// ## Transform Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransformsTask {
    #[prost(string, tag="1")]
    pub source: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub pipeline: ::core::option::Option<super::transforms::TransformPipeline>,
}
/// ## Top-level Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
    #[prost(message, optional, tag="1")]
    pub variable: ::core::option::Option<Variable>,
    #[prost(uint32, repeated, tag="2")]
    pub scope: ::prost::alloc::vec::Vec<u32>,
    #[prost(oneof="task::TaskKind", tags="3, 4, 5")]
    pub task_kind: ::core::option::Option<task::TaskKind>,
}
/// Nested message and enum types in `Task`.
pub mod task {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TaskKind {
        #[prost(message, tag="3")]
        Value(super::TaskValue),
        #[prost(message, tag="4")]
        Url(super::ScanUrlTask),
        #[prost(message, tag="5")]
        Transforms(super::TransformsTask),
    }
}
/// ## Task Graph
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IncomingEdge {
    #[prost(uint32, tag="1")]
    pub source: u32,
    #[prost(uint32, optional, tag="2")]
    pub output: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutgoingEdge {
    #[prost(uint32, tag="1")]
    pub target: u32,
    #[prost(bool, tag="2")]
    pub propagate: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskNode {
    #[prost(message, optional, tag="1")]
    pub task: ::core::option::Option<Task>,
    #[prost(message, repeated, tag="2")]
    pub incoming: ::prost::alloc::vec::Vec<IncomingEdge>,
    #[prost(message, repeated, tag="3")]
    pub outgoing: ::prost::alloc::vec::Vec<OutgoingEdge>,
    #[prost(uint64, tag="4")]
    pub id_fingerprint: u64,
    #[prost(uint64, tag="5")]
    pub state_fingerprint: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskGraph {
    #[prost(message, repeated, tag="1")]
    pub nodes: ::prost::alloc::vec::Vec<TaskNode>,
}
/// ## Variable
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum VariableNamespace {
    Signal = 0,
    Data = 1,
    Scale = 2,
}
