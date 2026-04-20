'use strict';

const { transformSource } = require('./compiler/transform.js');
const { __waymarkRegisterAction } = require('./runtime/actions.js');
const { __waymarkRunCompiled, Workflow } = require('./runtime/workflow.js');
const { withWaymark } = require('./with-waymark.js');

module.exports = {
  __waymarkRegisterAction,
  Workflow,
  __waymarkRunCompiled,
  __internal: {
    transformSource
  },
  withWaymark
};
