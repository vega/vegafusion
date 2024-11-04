const CopyWebpackPlugin = require("copy-webpack-plugin");
const MonacoWebpackPlugin = require('monaco-editor-webpack-plugin');
const path = require('path');

module.exports = {
  entry: "./src/bootstrap.js",
  output: {
    path: path.resolve(__dirname, "dist"),
    filename: "bootstrap.js",
  },
  mode: "development",
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader']
      },
      {
        test: /\.svg$/,
        loader: 'svg-inline-loader'
      },
      // {
      //   test: /\.wasm$/,
      //   type: "webassembly/async",
      // }
    ]
  },
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        'src/index.html',
        // {
        //   from: '../../vegafusion-wasm/pkg/workerHelpers.worker.js',
        //   to: 'pkg/workerHelpers.worker.js'
        // }
        // {
        //   from: '../../vegafusion-wasm/pkg',
        //   to: 'pkg'
        // }
      ]
    }),
    new MonacoWebpackPlugin({
      languages: ['typescript', 'javascript', 'css', "json"]
    })
  ],
  experiments: {
    topLevelAwait: true,
    asyncWebAssembly: true,
  }
};
