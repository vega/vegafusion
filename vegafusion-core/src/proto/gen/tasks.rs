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
