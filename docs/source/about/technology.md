# Technology
## VegaFusion Technology Stack
VegaFusion uses a fairly diverse technology stack. The planner and runtime are both implemented in Rust with wrappers for Python and JavaScript.

The Task Graph specifications are defined as protocol buffer messages. The [prost](https://github.com/tokio-rs/prost) library is used to generate Rust data structures from these protocol buffer messages.  When Arrow tables appear as task graph root values, they are serialized inside the protocol buffer specification using the [Apache Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc).  The binary representation of the task graph protocol buffer message is what is transferred across the Jupyter Comms protocol.

## DataFusion integration
[Apache DataFusion](https://github.com/apache/datafusion) is an SQL compatible query engine that integrates with the Rust implementation of Apache Arrow.  VegaFusion uses DataFusion to implement many of the Vega transforms, and it compiles the Vega expression language directly into the DataFusion expression language.  In addition to being very fast, a particularly powerful characteristic of DataFusion is that it provides many interfaces that can be extended with custom Rust logic.  For example, VegaFusion defines a few custom UDFs that are designed to implement the precise semantics of the Vega transforms and the Vega expression language.
