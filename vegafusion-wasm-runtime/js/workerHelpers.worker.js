const { VegaFusionRuntimeHandle } = await import('./vegafusion_wasm_runtime.js');

const runtime = new VegaFusionRuntimeHandle();

onmessage = async (msg) => {
  console.log("Worker called", msg);
  postMessage(await runtime.query(msg.data));
};
