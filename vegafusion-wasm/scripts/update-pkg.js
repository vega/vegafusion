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
  "bootstrap": "^5.1.3",
  "grpc-web": "^1.3.1",
  "lodash": "^4.17.21",
  "vega": "^5.22.1",
  "vega-tooltip": "^0.27.0",
  "vega-util": "^1.17.0"
};

// Write back to pkg/package.json
fs.writeFileSync(pkgPath, JSON.stringify(pkg, null, 2)); 