/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// From: https://github.com/RReverser/wasm-bindgen-rayon/blob/main/src/workerHelpers.worker.js

// Note: our JS should have been generated in
// `[out-dir]/snippets/vegafusion_wasm-[hash]/workerHelpers.worker.js`,
// resolve the main module via `../../..`.

// Use URL.createObjectURL approach instead of direct import
let wasmModule = null;

onmessage = async ({ data: { initData } }) => {
  console.log("Initializing worker WASM runtime");
  if (!wasmModule) {
    // Dynamic import that happens after the worker starts
    const module = await import('../../../vegafusion_wasm.js');
    wasmModule = module;
  }
  await wasmModule.default(initData);
  postMessage(true);
  console.log("finished initializing worker WASM runtime");
  wasmModule.wbg_vegafusion_start_worker();
};