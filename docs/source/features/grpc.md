# gRPC
The VegaFusion Runtime can run as a [gRPC](https://grpc.io/) service, which makes it possible for multiple clients to connect to the same runtime, and share a cache (See [How it Works](../about/how_it_works) for more details). This also makes it possible for the Runtime to reside on a different host than the client.

:::{warning}
VegaFusion's gRPC server does not currently support authentication. If you use it with untrusted Vega specifications, lock down the server process with `--no-allowed-urls`, `--allowed-base-url`, `--base-url`, or `--no-base-url`, and apply any additional isolation your deployment requires.

URL policy is enforced against the initial resolved URL only. VegaFusion does not re-check redirect destinations after a fetch begins.
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

The server process owns URL resolution and access policy for all gRPC clients. For example:

```
vegafusion-server \
  --port 50051 \
  --base-url https://cdn.jsdelivr.net/npm/vega-datasets@v2.9.0/ \
  --allowed-base-url https://cdn.jsdelivr.net/
```

## Python
The `vf.runtime.grpc_connect` method is used to connect the Python client to a VegaFusion Server instance.

For example, to connect to a server running locally on port 50051

```
import vegafusion as vf
vf.runtime.grpc_connect("http://127.0.0.1:50051")
```

This will cause all VegaFusion runtime operations to be dispatched to the VegaFusion Server.

See [grpc.py](https://github.com/vega/vegafusion/tree/main/examples/python-examples/grpc.py) for a complete example.

## Rust
The `GrpcVegaFusionRuntime` struct is an alternative to the `VegaFusionRuntime` struct that provides the same interface, but connects to a VegaFusion Server.

See [grpc.rs](https://github.com/vega/vegafusion/tree/main/examples/rust-examples/examples/grpc.rs) for a complete example.

## JavaScript
The `vegafusion-wasm` package can connect to an instance of VegaFusion Server over [gRPC-Web](https://github.com/grpc/grpc-web). 

See the [editor-demo](https://github.com/vega/vegafusion/tree/main/examples/editor-demo/README.md) example for more information.
