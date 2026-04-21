'use strict';

const { transformSource } = require('../compiler/transform.js');

module.exports = function waymarkLoader(source) {
  this.cacheable && this.cacheable();

  const result = transformSource(source, {
    projectRoot: this.rootContext || process.cwd(),
    resourcePath: this.resourcePath
  });

  for (const dependency of result.dependencies || []) {
    this.addDependency(dependency);
  }

  return result.code;
};
