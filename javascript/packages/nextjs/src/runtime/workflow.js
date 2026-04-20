'use strict';

const { runCompiledWorkflow } = require('./bridge.js');

class Workflow {
  async run() {
    throw new Error('Workflow.run() must be implemented by the Waymark compiler');
  }

  async runAction(awaitable, _options) {
    return await awaitable;
  }
}

async function __waymarkRunCompiled(workflowCtor, args) {
  return await runCompiledWorkflow(workflowCtor, args);
}

module.exports = {
  Workflow,
  __waymarkRunCompiled
};
