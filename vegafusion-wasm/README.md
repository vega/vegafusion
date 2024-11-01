# vegafusion-wasm
Wasm library for interfacing with VegaFusion.

For more information, see the VegaFusion repository at https://github.com/hex-inc/vegafusion

## Usage
Example usage with the VegaFusion server running locally on port 50051, with grpc-enabled:

```
./vegafusion-server --port 50051 --web
```

```javascript
const { vegaFusionEmbed, makeGrpcSendMessageFn } = await import("vegafusion-wasm");
import * as grpcWeb from 'grpc-web';

// Replace with your VegaFusion server hostname
const hostname = 'http://127.0.0.1:50051';
let client = new grpcWeb.GrpcWebClientBase({format: "binary"});
let send_message_grpc = makeGrpcSendMessageFn(client, hostname);

let element = document.getElementById("vega-chart");
let config = {
    verbose: false,
    debounce_wait: 30,
    debounce_max_wait: 60,
    embed_opts: {
        mode: "vega",
    },
};
let chart_handle = await vegaFusionEmbed(
    element,
    spec,  // Replace with your Vega spec
    send_message_grpc,
    config,
);
```

## webpack config

To use this library in a webpack project, you need to add the following [experimental options](https://webpack.js.org/configuration/experiments/) to your webpack config:

```
experiments: {
    asyncWebAssembly: true,
    topLevelAwait: true,
}
```