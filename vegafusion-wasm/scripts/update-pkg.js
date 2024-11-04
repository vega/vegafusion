/**
 * Post-build script for vegafusion-wasm package
 * 
 * This script runs after wasm-pack builds the WebAssembly module and generates the initial package.json.
 * It adds the npm dependencies required by the package. We handle dependencies this way because wasm-pack 
 * has issues parsing package.json files with certain fields, like dependencies.
 * 
 * Usage: node scripts/update-pkg.js
 * Should be run as part of the build process: wasm-pack build --release && node scripts/update-pkg.js
 */

const fs = require('fs');
const path = require('path');

// Read the generated package.json
const pkgPath = path.join(__dirname, '../pkg/package.json');
const pkg = require(pkgPath);

// Add your dependencies
pkg.dependencies = {
  "grpc-web": "^1.5.0",
  "lodash": "^4.17.21",
  "vega-embed": "^6.26.0",
  "vega-util": "^1.17.0"
};

// // Create js directory in pkg if it doesn't exist
// const pkgJsDir = path.join(__dirname, '../pkg/js');
// if (!fs.existsSync(pkgJsDir)) {
//     fs.mkdirSync(pkgJsDir, { recursive: true });
// }

// // Copy workerHelpers.worker.js to pkg directory
// const workerSrc = path.join(__dirname, '../js/workerHelpers.worker.js');
// const workerDest = path.join(__dirname, 'workerHelpers.worker.js');
// fs.copyFileSync(workerSrc, workerDest);

// Write back to pkg/package.json
fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2)); 
