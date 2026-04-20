'use strict';

const { syncActionBootstrap } = require('../compiler/bootstrap.js');

class WaymarkBootstrapPlugin {
  constructor(options = {}) {
    this.projectRoot = options.projectRoot || process.cwd();
    this.scanState = options.initialScan || null;
  }

  apply(compiler) {
    const sync = () => {
      this.scanState = syncActionBootstrap(this.projectRoot);
    };

    compiler.hooks.beforeRun.tap('WaymarkBootstrapPlugin', sync);
    compiler.hooks.watchRun.tap('WaymarkBootstrapPlugin', sync);
    compiler.hooks.thisCompilation.tap('WaymarkBootstrapPlugin', (compilation) => {
      if (!this.scanState) {
        sync();
      }

      for (const directory of this.scanState.directories) {
        compilation.contextDependencies.add(directory);
      }
    });
  }
}

module.exports = {
  WaymarkBootstrapPlugin
};
