'use strict';

const loaderPath = require.resolve('./loader/waymark-loader.js');

function withWaymark(nextConfig = {}, pluginOptions = {}) {
  return {
    ...nextConfig,
    webpack(config, options) {
      if (options.isServer) {
        config.module.rules.push({
          test: /\.[jt]sx?$/,
          exclude: /node_modules/,
          use: [
            {
              loader: loaderPath,
              options: {
                projectRoot: pluginOptions.projectRoot || process.cwd()
              }
            }
          ]
        });
      }

      if (typeof nextConfig.webpack === 'function') {
        return nextConfig.webpack(config, options);
      }

      return config;
    }
  };
}

module.exports = {
  withWaymark
};
