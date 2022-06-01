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
    #[prost(string, optional, tag="1")]
    pub r#type: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="4")]
    pub property: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, repeated, tag="5")]
    pub header: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, optional, tag="6")]
    pub delimiter: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag="7")]
    pub feature: ::core::option::Option<::prost::alloc::string::String>,
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
pub struct DataUrlTask {
    #[prost(int32, tag="3")]
    pub batch_size: i32,
    #[prost(message, optional, tag="4")]
    pub format_type: ::core::option::Option<ScanUrlFormat>,
    #[prost(message, optional, tag="5")]
    pub pipeline: ::core::option::Option<super::transforms::TransformPipeline>,
    #[prost(oneof="data_url_task::Url", tags="1, 2")]
    pub url: ::core::option::Option<data_url_task::Url>,
}
/// Nested message and enum types in `DataUrlTask`.
pub mod data_url_task {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Url {
        #[prost(string, tag="1")]
        String(::prost::alloc::string::String),
        #[prost(message, tag="2")]
        Expr(super::super::expression::Expression),
    }
}
/// ## Inline values task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataValuesTask {
    #[prost(bytes="vec", tag="1")]
    pub values: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag="2")]
    pub format_type: ::core::option::Option<ScanUrlFormat>,
    #[prost(message, optional, tag="3")]
    pub pipeline: ::core::option::Option<super::transforms::TransformPipeline>,
}
/// ## Transform Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataSourceTask {
    #[prost(string, tag="1")]
    pub source: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub pipeline: ::core::option::Option<super::transforms::TransformPipeline>,
}
/// ## Signal Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignalTask {
    #[prost(message, optional, tag="2")]
    pub expr: ::core::option::Option<super::expression::Expression>,
}
/// ## Timezone config
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TzConfig {
    #[prost(string, tag="1")]
    pub local_tz: ::prost::alloc::string::String,
    #[prost(string, optional, tag="2")]
    pub default_input_tz: ::core::option::Option<::prost::alloc::string::String>,
}
/// ## Top-level Task
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
    #[prost(message, optional, tag="1")]
    pub variable: ::core::option::Option<Variable>,
    #[prost(uint32, repeated, tag="2")]
    pub scope: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, tag="8")]
    pub tz_config: ::core::option::Option<TzConfig>,
    #[prost(oneof="task::TaskKind", tags="3, 4, 5, 6, 7")]
    pub task_kind: ::core::option::Option<task::TaskKind>,
}
/// Nested message and enum types in `Task`.
pub mod task {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TaskKind {
        #[prost(message, tag="3")]
        Value(super::TaskValue),
        #[prost(message, tag="4")]
        DataValues(super::DataValuesTask),
        #[prost(message, tag="5")]
        DataUrl(super::DataUrlTask),
        #[prost(message, tag="6")]
        DataSource(super::DataSourceTask),
        #[prost(message, tag="7")]
        Signal(super::SignalTask),
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
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NodeValueIndex {
    #[prost(uint32, tag="1")]
    pub node_index: u32,
    #[prost(uint32, optional, tag="2")]
    pub output_index: ::core::option::Option<u32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskGraphValueRequest {
    #[prost(message, optional, tag="1")]
    pub task_graph: ::core::option::Option<TaskGraph>,
    #[prost(message, repeated, tag="2")]
    pub indices: ::prost::alloc::vec::Vec<NodeValueIndex>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseTaskValue {
    #[prost(message, optional, tag="1")]
    pub variable: ::core::option::Option<Variable>,
    #[prost(uint32, repeated, tag="2")]
    pub scope: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, tag="3")]
    pub value: ::core::option::Option<TaskValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskGraphValueResponse {
    #[prost(message, repeated, tag="1")]
    pub response_values: ::prost::alloc::vec::Vec<ResponseTaskValue>,
}
/// ## Variable
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum VariableNamespace {
    Signal = 0,
    Data = 1,
    Scale = 2,
}
