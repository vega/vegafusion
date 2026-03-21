### Chart Editor

This example is a simple chart editor that relies on the vegafusion-wasm package, and connects to the 
vegafusion server over gRPC-Web 

Launch gRPC-Web server with:
```
./vegafusion-server --port 50051 --web
```

Add `--base-url`, `--no-base-url`, `--allowed-base-url`, or `--no-allowed-urls`
to control how the server resolves and accesses external data URLs.
Policy checks apply to the initial resolved URL only; redirect destinations are
not re-checked after a fetch begins.

Build and launch editor with
```
npm install
npm run start
```
