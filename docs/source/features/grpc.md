# gRPC
The VegaFusion Runtime can run as a [gRPC](https://grpc.io/) service, which makes it possible for multiple clients to connect to the same runtime, and share a cache (See [How it Works](../about/how_it_works) for more details). This also makes it possible for the Runtime to reside on a different host than the client.

:::{warning}
VegaFusion's gRPC server does not currently support authentication, and chart specifications may reference the local file system of the machine running the server. It is not currently recommended to use VegaFusion server with untrusted Vega specifications unless other measures are taken to isolate the service.
:::

## VegaFusion Server
The gRPC service is called VegaFusion Server. Executables for common architectures are published as [GitHub Release](https://github.com/vega/vegafusion/releases) artifacts. The server can also be compiled from source using

```
cargo install vegafusion-server
```

The server may then be launched using a particular port as follows:

```
vegafusion-server --port 50051
```

## Python
The `vf.runtime.grpc_connect` method is used to connect the Python client to a VegaFusion Server instance.

For example, to connect to a server running locally on port 50051

```
import vegafusion as vf
vf.runtime.grpc_connect("http://127.0.0.1:50051")
```

This will cause all VegaFusion runtime operations to be dispatched to the VegaFusion Server.

See [grpc.py](https://github.com/vega/vegafusion/tree/v2/examples/python-examples/grpc.py) for a complete example.

## Rust
The `GrpcVegaFusionRuntime` struct is an alternative to the `VegaFusionRuntime` struct that provides the same interface, but connects to a VegaFusion Server.

See [grpc.rs](https://github.com/vega/vegafusion/tree/v2/examples/rust-examples/examples/grpc.rs) for a complete example.

## JavaScript
The `vegafusion-wasm` package can connect to an instance of VegaFusion Server over [gRPC-Web](https://github.com/grpc/grpc-web). 

See the [editor-demo](https://github.com/vega/vegafusion/tree/v2/examples/editor-demo/README.md) example for more information.
