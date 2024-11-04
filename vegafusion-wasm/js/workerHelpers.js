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

// From: https://raw.githubusercontent.com/RReverser/wasm-bindgen-rayon/refs/heads/main/src/workerHelpers.js

// Note: this is never used, but necessary to prevent a bug in Firefox
// (https://bugzilla.mozilla.org/show_bug.cgi?id=1702191) where it collects
// Web Workers that have a shared WebAssembly memory with the main thread,
// but are not explicitly rooted via a `Worker` instance.
//
// By storing them in a variable, we can keep `Worker` objects around and
// prevent them from getting GC-d.
let _workers;
// In your main file
// import initWbg, { wbg_vegafusion_start_worker } from '../../../vegafusion_wasm.js';

export async function startWorkers(module) {
  const worker = new Worker(
    new URL('./workerHelpers.worker.js', import.meta.url),
    {
      type: 'module'
    }
  );
  
  // Just pass the initialization data, not the functions
  worker.postMessage({
    initData: module
  });

  await new Promise(resolve =>
    worker.addEventListener('message', resolve, { once: true })
  );

  _workers = [worker];
}