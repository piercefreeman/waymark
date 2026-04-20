'use strict';

const { syncActionBootstrap } = require('./compiler/bootstrap.js');
const loaderPath = require.resolve('./loader/waymark-loader.js');
const { WaymarkBootstrapPlugin } = require('./webpack/waymark-bootstrap-plugin.js');

function withWaymark(nextConfig = {}, pluginOptions = {}) {
  const projectRoot = pluginOptions.projectRoot || process.cwd();
  const initialScan = syncActionBootstrap(projectRoot);

  return {
    ...nextConfig,
    webpack(config, options) {
      const nextWebpackConfig = typeof nextConfig.webpack === 'function'
        ? nextConfig.webpack(config, options)
        : config;

      if (options.isServer) {
        nextWebpackConfig.module = nextWebpackConfig.module || {};
        nextWebpackConfig.module.rules = nextWebpackConfig.module.rules || [];
        nextWebpackConfig.module.rules.push({
          test: /\.[jt]sx?$/,
          exclude: /node_modules/,
          use: [
            {
              loader: loaderPath,
              options: {
                projectRoot
              }
            }
          ]
        });

        nextWebpackConfig.plugins = nextWebpackConfig.plugins || [];
        nextWebpackConfig.plugins.push(
          new WaymarkBootstrapPlugin({
            initialScan,
            projectRoot
          })
        );
      }

      return nextWebpackConfig;
    }
  };
}

module.exports = {
  withWaymark
};
