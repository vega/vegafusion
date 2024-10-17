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
      }, {
        test: /\.svg$/,
        loader: 'svg-inline-loader'
      },
    ]
  },
  plugins: [
    new CopyWebpackPlugin({patterns: ['src/index.html']}),
    new MonacoWebpackPlugin({
      languages: ['typescript', 'javascript', 'css', "json"]
    })
  ],
  experiments: {
    topLevelAwait: true,
    asyncWebAssembly: true,
  }
};
