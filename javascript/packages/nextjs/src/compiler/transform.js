'use strict';

const fs = require('node:fs');
const path = require('node:path');

const generate = require('@babel/generator').default;
const parser = require('@babel/parser');
const traverse = require('@babel/traverse').default;
const t = require('@babel/types');

const ir = require('../generated/ast_pb.js');
const { hashProgramBytes } = require('../runtime/bridge.js');

const PARSER_PLUGINS = [
  'typescript',
  'jsx',
  'classProperties',
  'classPrivateProperties',
  'classPrivateMethods',
  'topLevelAwait'
];

function transformSource(source, options = {}) {
  const resourcePath = options.resourcePath || path.join(process.cwd(), 'workflow.ts');
  const projectRoot = options.projectRoot || process.cwd();

  const ast = parser.parse(source, {
    sourceType: 'module',
    plugins: PARSER_PLUGINS
  });

  const localActions = collectLocalActionExports(ast, resourcePath, projectRoot);
  const importedActionInfo = collectImportedActionBindings(ast, resourcePath, projectRoot);
  const actionBindings = new Map([
    ...Array.from(localActions.entries()),
    ...Array.from(importedActionInfo.bindings.entries())
  ]);

  const workflowImportNames = collectWorkflowImportNames(ast);
  const hasLocalActions = localActions.size > 0;
  const hasWorkflows = workflowImportNames.size > 0;

  if (!hasLocalActions && !hasWorkflows) {
    return {
      code: source,
      dependencies: importedActionInfo.dependencies,
      transformed: false
    };
  }

  if (hasLocalActions) {
    injectActionRegistrations(ast, localActions);
  }

  const compiledClasses = [];

  if (hasWorkflows) {
    traverse(ast, {
      ClassDeclaration(classPath) {
        if (!isWorkflowClass(classPath.node, workflowImportNames)) {
          return;
        }

        const runMethod = findRunMethod(classPath.node);
        if (!runMethod) {
          throw buildCodeFrameError(resourcePath, classPath.node, 'Workflow class must define async run(...)');
        }

        const compiled = compileWorkflowClass(classPath.node, runMethod, actionBindings, {
          projectRoot,
          resourcePath
        });

        rewriteWorkflowClass(classPath.node, runMethod, compiled);
        compiledClasses.push(compiled);
      }
    });
  }

  const helperNames = [];
  if (compiledClasses.length > 0) {
    helperNames.push('__waymarkRunCompiled');
  }
  if (hasLocalActions) {
    helperNames.push('__waymarkRegisterAction');
  }
  ensureWaymarkImports(ast, helperNames);

  return {
    code: generate(ast, { comments: true }).code,
    dependencies: importedActionInfo.dependencies,
    transformed: true,
    workflows: compiledClasses
  };
}

function collectWorkflowImportNames(ast) {
  const names = new Set();

  for (const node of ast.program.body) {
    if (!t.isImportDeclaration(node) || node.source.value !== '@waymark/nextjs') {
      continue;
    }

    for (const specifier of node.specifiers) {
      if (
        t.isImportSpecifier(specifier) &&
        t.isIdentifier(specifier.imported) &&
        specifier.imported.name === 'Workflow'
      ) {
        names.add(specifier.local.name);
      }
    }
  }

  return names;
}

function collectLocalActionExports(ast, resourcePath, projectRoot) {
  const actions = new Map();

  for (const node of ast.program.body) {
    if (!t.isExportNamedDeclaration(node) || !t.isFunctionDeclaration(node.declaration)) {
      continue;
    }

    const fn = node.declaration;
    if (!fn.id || !fn.async || !hasUseActionComment(node, fn)) {
      continue;
    }

    actions.set(fn.id.name, {
      actionName: fn.id.name,
      moduleName: moduleNameForCurrentFile(resourcePath, projectRoot),
      paramNames: extractParamNames(fn.params, resourcePath, fn)
    });
  }

  return actions;
}

function collectImportedActionBindings(ast, resourcePath, projectRoot) {
  const bindings = new Map();
  const dependencies = [];
  const actionCache = new Map();

  for (const node of ast.program.body) {
    if (!t.isImportDeclaration(node) || node.source.value === '@waymark/nextjs') {
      continue;
    }

    const resolvedPath = resolveImportPath(node.source.value, resourcePath, projectRoot);
    if (!resolvedPath) {
      continue;
    }

    const exportedActions = loadActionExports(resolvedPath, projectRoot, actionCache);
    if (exportedActions.size === 0) {
      continue;
    }

    dependencies.push(resolvedPath);

    for (const specifier of node.specifiers) {
      if (!t.isImportSpecifier(specifier) || !t.isIdentifier(specifier.imported)) {
        continue;
      }

      const exportedAction = exportedActions.get(specifier.imported.name);
      if (!exportedAction) {
        continue;
      }

      bindings.set(specifier.local.name, {
        actionName: exportedAction.actionName,
        moduleName: exportedAction.moduleName,
        paramNames: exportedAction.paramNames
      });
    }
  }

  return {
    bindings,
    dependencies
  };
}

function loadActionExports(modulePath, projectRoot, cache) {
  if (cache.has(modulePath)) {
    return cache.get(modulePath);
  }

  const source = fs.readFileSync(modulePath, 'utf8');
  const ast = parser.parse(source, {
    sourceType: 'module',
    plugins: PARSER_PLUGINS
  });
  const actions = collectLocalActionExports(ast, modulePath, projectRoot);
  cache.set(modulePath, actions);
  return actions;
}

function resolveImportPath(source, resourcePath, projectRoot) {
  let candidateBase = null;

  if (source.startsWith('./') || source.startsWith('../')) {
    candidateBase = path.resolve(path.dirname(resourcePath), source);
  } else if (source.startsWith('@/')) {
    candidateBase = path.resolve(projectRoot, source.slice(2));
  } else if (path.isAbsolute(source)) {
    candidateBase = source;
  }

  if (!candidateBase) {
    return null;
  }

  const candidates = [
    candidateBase,
    `${candidateBase}.ts`,
    `${candidateBase}.tsx`,
    `${candidateBase}.js`,
    `${candidateBase}.jsx`,
    path.join(candidateBase, 'index.ts'),
    path.join(candidateBase, 'index.tsx'),
    path.join(candidateBase, 'index.js'),
    path.join(candidateBase, 'index.jsx')
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
      return candidate;
    }
  }

  return null;
}

function isWorkflowClass(node, workflowImportNames) {
  return (
    t.isIdentifier(node.id) &&
    t.isIdentifier(node.superClass) &&
    workflowImportNames.has(node.superClass.name)
  );
}

function findRunMethod(classNode) {
  for (const member of classNode.body.body) {
    if (t.isClassMethod(member) && t.isIdentifier(member.key) && member.key.name === 'run') {
      return member;
    }
  }

  return null;
}

function compileWorkflowClass(classNode, runMethod, actionBindings, options) {
  if (!runMethod.async) {
    throw buildCodeFrameError(options.resourcePath, runMethod, 'Workflow run() must be async');
  }

  const paramNames = extractParamNames(runMethod.params, options.resourcePath, runMethod);
  const program = new ir.Program();
  const functionDef = new ir.FunctionDef();
  functionDef.setName('main');

  const io = new ir.IoDecl();
  io.setInputsList(paramNames);
  io.setOutputsList(['result']);
  functionDef.setIo(io);

  functionDef.setBody(
    compileBlock(runMethod.body.body, {
      actionBindings,
      loopVariables: new Set(),
      resourcePath: options.resourcePath,
      tempState: {
        nextSyntheticNameIndex: 0
      }
    })
  );

  program.addFunctions(functionDef);

  const programBytes = Buffer.from(program.serializeBinary());
  const irHash = hashProgramBytes(programBytes);

  return {
    className: classNode.id.name,
    workflowName: classNode.id.name.toLowerCase(),
    inputNames: paramNames,
    programBase64: programBytes.toString('base64'),
    irHash,
    workflowVersion: irHash,
    concurrent: false,
    params: runMethod.params
  };
}

function compileBlock(statements, context) {
  const block = new ir.Block();
  for (const statement of statements) {
    for (const compiledStatement of compileStatement(statement, context)) {
      block.addStatements(compiledStatement);
    }
  }
  return block;
}

function compileStatement(node, context) {
  if (t.isVariableDeclaration(node)) {
    return node.declarations.map((declarator) => compileVariableDeclarator(declarator, context));
  }

  if (t.isReturnStatement(node)) {
    if (node.argument) {
      const normalizedReturn = maybeNormalizeReturnExpression(node.argument, context);
      if (normalizedReturn) {
        return normalizedReturn;
      }
    }

    return [buildReturnStatement(node.argument ? compileExpression(node.argument, context) : null)];
  }

  if (t.isExpressionStatement(node)) {
    const actionCall = maybeCompileActionCall(unwrapAwait(node.expression), context);
    if (actionCall) {
      const statement = new ir.Statement();
      statement.setActionCall(actionCall);
      return [statement];
    }

    const statement = new ir.Statement();
    const exprStmt = new ir.ExprStmt();
    exprStmt.setExpr(compileExpression(node.expression, context));
    statement.setExprStmt(exprStmt);
    return [statement];
  }

  if (t.isIfStatement(node)) {
    const statement = new ir.Statement();
    statement.setConditional(compileConditional(node, context));
    return [statement];
  }

  if (t.isForOfStatement(node)) {
    const statement = new ir.Statement();
    statement.setForLoop(compileForOfStatement(node, context));
    return [statement];
  }

  throw buildCodeFrameError(context.resourcePath, node, `Unsupported statement: ${node.type}`);
}

function compileVariableDeclarator(node, context) {
  if (!t.isIdentifier(node.id)) {
    throw buildCodeFrameError(context.resourcePath, node, 'Only identifier assignments are supported');
  }
  if (!node.init) {
    throw buildCodeFrameError(context.resourcePath, node, 'Variable declarations must have an initializer');
  }

  const assignment = new ir.Assignment();
  assignment.setTargetsList([node.id.name]);
  assignment.setValue(compileExpression(node.init, context));

  const statement = new ir.Statement();
  statement.setAssignment(assignment);
  return statement;
}

function compileConditional(node, context) {
  const conditional = new ir.Conditional();

  const ifBranch = new ir.IfBranch();
  ifBranch.setCondition(compileExpression(node.test, context));
  ifBranch.setBlockBody(compileBlock(asBlockStatements(node.consequent), context));
  conditional.setIfBranch(ifBranch);

  let currentAlternate = node.alternate;
  while (t.isIfStatement(currentAlternate)) {
    const elifBranch = new ir.ElifBranch();
    elifBranch.setCondition(compileExpression(currentAlternate.test, context));
    elifBranch.setBlockBody(compileBlock(asBlockStatements(currentAlternate.consequent), context));
    conditional.addElifBranches(elifBranch);
    currentAlternate = currentAlternate.alternate;
  }

  if (currentAlternate) {
    const elseBranch = new ir.ElseBranch();
    elseBranch.setBlockBody(compileBlock(asBlockStatements(currentAlternate), context));
    conditional.setElseBranch(elseBranch);
  }

  return conditional;
}

function compileForOfStatement(node, context) {
  const loop = new ir.ForLoop();
  const loopVar = extractForLoopVariable(node.left, context.resourcePath);
  loop.setLoopVarsList([loopVar]);
  loop.setIterable(compileExpression(node.right, context));

  const nestedContext = {
    ...context,
    loopVariables: new Set([...context.loopVariables, loopVar])
  };
  loop.setBlockBody(compileBlock(asBlockStatements(node.body), nestedContext));
  return loop;
}

function compileExpression(node, context) {
  const target = unwrapAwait(node);

  if (t.isIdentifier(target)) {
    return variableExpr(target.name);
  }

  if (t.isStringLiteral(target)) {
    return literalExpr((literal) => literal.setStringValue(target.value));
  }

  if (t.isNumericLiteral(target)) {
    if (Number.isInteger(target.value)) {
      return literalExpr((literal) => literal.setIntValue(target.value));
    }
    return literalExpr((literal) => literal.setFloatValue(target.value));
  }

  if (t.isBooleanLiteral(target)) {
    return literalExpr((literal) => literal.setBoolValue(target.value));
  }

  if (t.isNullLiteral(target)) {
    return literalExpr((literal) => literal.setIsNone(true));
  }

  if (t.isObjectExpression(target)) {
    const dict = new ir.DictExpr();
    for (const property of target.properties) {
      if (!t.isObjectProperty(property) || property.computed) {
        throw buildCodeFrameError(context.resourcePath, property, 'Only simple object properties are supported');
      }

      const entry = new ir.DictEntry();
      entry.setKey(objectKeyToExpr(property.key));
      entry.setValue(compileExpression(property.value, context));
      dict.addEntries(entry);
    }

    const expr = new ir.Expr();
    expr.setDict(dict);
    return expr;
  }

  if (t.isArrayExpression(target)) {
    const list = new ir.ListExpr();
    for (const element of target.elements) {
      if (!element || t.isSpreadElement(element)) {
        throw buildCodeFrameError(context.resourcePath, target, 'Array spreads are not supported');
      }
      list.addElements(compileExpression(element, context));
    }

    const expr = new ir.Expr();
    expr.setList(list);
    return expr;
  }

  if (t.isMemberExpression(target)) {
    if (target.computed) {
      const index = new ir.IndexAccess();
      index.setObject(compileExpression(target.object, context));
      index.setIndex(compileExpression(target.property, context));

      const expr = new ir.Expr();
      expr.setIndex(index);
      return expr;
    }

    if (!t.isIdentifier(target.property)) {
      throw buildCodeFrameError(context.resourcePath, target, 'Only identifier member access is supported');
    }

    const dot = new ir.DotAccess();
    dot.setObject(compileExpression(target.object, context));
    dot.setAttribute(target.property.name);

    const expr = new ir.Expr();
    expr.setDot(dot);
    return expr;
  }

  if (t.isUnaryExpression(target)) {
    const unary = new ir.UnaryOp();
    if (target.operator === '-') {
      unary.setOp(ir.UnaryOperator.UNARY_OP_NEG);
    } else if (target.operator === '!') {
      unary.setOp(ir.UnaryOperator.UNARY_OP_NOT);
    } else {
      throw buildCodeFrameError(context.resourcePath, target, `Unsupported unary operator: ${target.operator}`);
    }
    unary.setOperand(compileExpression(target.argument, context));

    const expr = new ir.Expr();
    expr.setUnaryOp(unary);
    return expr;
  }

  if (t.isBinaryExpression(target) || t.isLogicalExpression(target)) {
    const binary = new ir.BinaryOp();
    binary.setLeft(compileExpression(target.left, context));
    binary.setRight(compileExpression(target.right, context));
    binary.setOp(binaryOperatorFor(target.operator, context.resourcePath, target));

    const expr = new ir.Expr();
    expr.setBinaryOp(binary);
    return expr;
  }

  if (t.isCallExpression(target)) {
    const actionCall = maybeCompileActionCall(target, context);
    if (actionCall) {
      const expr = new ir.Expr();
      expr.setActionCall(actionCall);
      return expr;
    }

    const spreadExpr = maybeCompileSpreadExpression(target, context);
    if (spreadExpr) {
      const expr = new ir.Expr();
      expr.setSpreadExpr(spreadExpr);
      return expr;
    }

    const parallelExpr = maybeCompileParallelExpression(target, context);
    if (parallelExpr) {
      const expr = new ir.Expr();
      expr.setParallelExpr(parallelExpr);
      return expr;
    }

    const functionCall = compileFunctionCall(target, context);
    const expr = new ir.Expr();
    expr.setFunctionCall(functionCall);
    return expr;
  }

  throw buildCodeFrameError(context.resourcePath, target, `Unsupported expression: ${target.type}`);
}

function maybeNormalizeReturnExpression(node, context) {
  const expr = compileExpression(node, context);

  switch (expr.getKindCase()) {
    case ir.Expr.KindCase.ACTION_CALL:
    case ir.Expr.KindCase.FUNCTION_CALL:
    case ir.Expr.KindCase.PARALLEL_EXPR:
    case ir.Expr.KindCase.SPREAD_EXPR: {
      const tempVarName = nextSyntheticName(context, 'returnTmp');
      return [
        buildAssignmentStatement(tempVarName, expr),
        buildReturnStatement(variableExpr(tempVarName))
      ];
    }
    default:
      return null;
  }
}

function maybeCompileActionCall(node, context) {
  if (!t.isCallExpression(node)) {
    return null;
  }

  if (isThisRunActionCall(node)) {
    if (node.arguments.length === 0) {
      throw buildCodeFrameError(context.resourcePath, node, 'this.runAction(...) requires an action call');
    }

    const innerCall = unwrapAwait(node.arguments[0]);
    const actionCall = maybeCompileActionCall(innerCall, context);
    if (!actionCall) {
      throw buildCodeFrameError(context.resourcePath, node, 'this.runAction(...) must wrap an action call');
    }

    if (node.arguments[1]) {
      for (const policy of parsePolicies(node.arguments[1], context)) {
        actionCall.addPolicies(policy);
      }
    }

    return actionCall;
  }

  if (!t.isIdentifier(node.callee)) {
    return null;
  }

  const actionMetadata = context.actionBindings.get(node.callee.name);
  if (!actionMetadata) {
    return null;
  }

  const actionCall = new ir.ActionCall();
  actionCall.setActionName(actionMetadata.actionName);
  actionCall.setModuleName(actionMetadata.moduleName);
  actionCall.setKwargsList(
    buildKwargsFromArguments(node.arguments, actionMetadata, context)
  );
  return actionCall;
}

function maybeCompileSpreadExpression(node, context) {
  if (!isPromiseAllCall(node) || node.arguments.length !== 1) {
    return null;
  }

  const [arg] = node.arguments;
  if (!t.isCallExpression(arg) || !isArrayMapCall(arg)) {
    return null;
  }

  const callback = arg.arguments[0];
  if (!(t.isArrowFunctionExpression(callback) || t.isFunctionExpression(callback))) {
    throw buildCodeFrameError(context.resourcePath, arg, 'Array.map(...) must use an inline callback');
  }

  if (callback.params.length !== 1 || !t.isIdentifier(callback.params[0])) {
    throw buildCodeFrameError(context.resourcePath, callback, 'Spread callbacks must accept exactly one identifier');
  }

  const loopVar = callback.params[0].name;
  const callbackBody = extractReturnedExpression(callback.body);
  const nestedContext = {
    ...context,
    loopVariables: new Set([...context.loopVariables, loopVar])
  };
  const actionCall = maybeCompileActionCall(unwrapAwait(callbackBody), nestedContext);
  if (!actionCall) {
    throw buildCodeFrameError(context.resourcePath, callbackBody, 'Spread callbacks must return an action call');
  }

  const spread = new ir.SpreadExpr();
  spread.setCollection(compileExpression(arg.callee.object, context));
  spread.setLoopVar(loopVar);
  spread.setAction(actionCall);
  return spread;
}

function maybeCompileParallelExpression(node, context) {
  if (!isPromiseAllCall(node) || node.arguments.length !== 1) {
    return null;
  }

  const [arg] = node.arguments;
  if (!t.isArrayExpression(arg)) {
    return null;
  }

  const parallel = new ir.ParallelExpr();
  for (const element of arg.elements) {
    if (!element || t.isSpreadElement(element)) {
      throw buildCodeFrameError(context.resourcePath, arg, 'Promise.all([...]) does not support spreads');
    }

    const call = compileParallelCall(unwrapAwait(element), context);
    parallel.addCalls(call);
  }
  return parallel;
}

function compileParallelCall(node, context) {
  const actionCall = maybeCompileActionCall(node, context);
  if (actionCall) {
    const call = new ir.Call();
    call.setAction(actionCall);
    return call;
  }

  if (!t.isCallExpression(node)) {
    throw buildCodeFrameError(context.resourcePath, node, 'Parallel expressions require calls');
  }

  const functionCall = compileFunctionCall(node, context);
  const call = new ir.Call();
  call.setFunction(functionCall);
  return call;
}

function compileFunctionCall(node, context) {
  if (!t.isIdentifier(node.callee)) {
    throw buildCodeFrameError(context.resourcePath, node, 'Only direct function calls are supported');
  }

  const functionCall = new ir.FunctionCall();
  functionCall.setName(node.callee.name);
  functionCall.setArgsList(node.arguments.map((arg) => {
    if (t.isSpreadElement(arg)) {
      throw buildCodeFrameError(context.resourcePath, arg, 'Function call spreads are not supported');
    }
    return compileExpression(arg, context);
  }));
  return functionCall;
}

function buildKwargsFromArguments(args, actionMetadata, context) {
  if (args.length > actionMetadata.paramNames.length) {
    throw buildCodeFrameError(
      context.resourcePath,
      args[actionMetadata.paramNames.length],
      `Action ${actionMetadata.actionName} received too many positional arguments`
    );
  }

  return args.map((arg, index) => {
    if (t.isSpreadElement(arg)) {
      throw buildCodeFrameError(context.resourcePath, arg, 'Action call spreads are not supported');
    }

    const kwarg = new ir.Kwarg();
    kwarg.setName(actionMetadata.paramNames[index]);
    kwarg.setValue(compileExpression(arg, context));
    return kwarg;
  });
}

function buildAssignmentStatement(targetName, expr) {
  const assignment = new ir.Assignment();
  assignment.setTargetsList([targetName]);
  assignment.setValue(expr);

  const statement = new ir.Statement();
  statement.setAssignment(assignment);
  return statement;
}

function buildReturnStatement(expr) {
  const statement = new ir.Statement();
  const returnStmt = new ir.ReturnStmt();
  if (expr) {
    returnStmt.setValue(expr);
  }
  statement.setReturnStmt(returnStmt);
  return statement;
}

function parsePolicies(node, context) {
  if (!t.isObjectExpression(node)) {
    throw buildCodeFrameError(context.resourcePath, node, 'Action policy options must be an object literal');
  }

  const policies = [];

  for (const property of node.properties) {
    if (!t.isObjectProperty(property) || property.computed) {
      throw buildCodeFrameError(context.resourcePath, property, 'Action policy keys must be simple identifiers');
    }

    const keyName = objectKeyName(property.key);
    if (keyName === 'retry') {
      policies.push(buildRetryPolicy(property.value, context));
      continue;
    }
    if (keyName === 'timeout') {
      policies.push(buildTimeoutPolicy(property.value, context));
      continue;
    }

    throw buildCodeFrameError(context.resourcePath, property, `Unsupported action policy key: ${keyName}`);
  }

  return policies;
}

function buildRetryPolicy(node, context) {
  if (!t.isObjectExpression(node)) {
    throw buildCodeFrameError(context.resourcePath, node, 'retry policy must be an object literal');
  }

  let attempts = null;
  let backoffSeconds = null;
  let exceptionTypes = [];

  for (const property of node.properties) {
    if (!t.isObjectProperty(property) || property.computed) {
      throw buildCodeFrameError(context.resourcePath, property, 'retry policy keys must be simple identifiers');
    }

    const keyName = objectKeyName(property.key);
    if (keyName === 'attempts') {
      if (!t.isNumericLiteral(property.value) || !Number.isInteger(property.value.value)) {
        throw buildCodeFrameError(context.resourcePath, property.value, 'retry.attempts must be an integer literal');
      }
      attempts = property.value.value;
      continue;
    }

    if (keyName === 'backoffSeconds') {
      if (!t.isNumericLiteral(property.value) || !Number.isInteger(property.value.value)) {
        throw buildCodeFrameError(context.resourcePath, property.value, 'retry.backoffSeconds must be an integer literal');
      }
      backoffSeconds = property.value.value;
      continue;
    }

    if (keyName === 'exceptionTypes') {
      if (!t.isArrayExpression(property.value)) {
        throw buildCodeFrameError(context.resourcePath, property.value, 'retry.exceptionTypes must be an array literal');
      }
      exceptionTypes = property.value.elements.map((element) => {
        if (!element || !t.isStringLiteral(element)) {
          throw buildCodeFrameError(context.resourcePath, property.value, 'retry.exceptionTypes entries must be string literals');
        }
        return element.value;
      });
      continue;
    }

    throw buildCodeFrameError(context.resourcePath, property, `Unsupported retry policy key: ${keyName}`);
  }

  if (attempts === null) {
    throw buildCodeFrameError(context.resourcePath, node, 'retry policy requires attempts');
  }

  const retryPolicy = new ir.RetryPolicy();
  retryPolicy.setExceptionTypesList(exceptionTypes);
  retryPolicy.setMaxRetries(Math.max(0, attempts - 1));
  if (backoffSeconds !== null) {
    const duration = new ir.Duration();
    duration.setSeconds(backoffSeconds);
    retryPolicy.setBackoff(duration);
  }

  const policy = new ir.PolicyBracket();
  policy.setRetry(retryPolicy);
  return policy;
}

function buildTimeoutPolicy(node, context) {
  const seconds = parseDurationLiteral(node, context);
  const duration = new ir.Duration();
  duration.setSeconds(seconds);

  const timeoutPolicy = new ir.TimeoutPolicy();
  timeoutPolicy.setTimeout(duration);

  const policy = new ir.PolicyBracket();
  policy.setTimeout(timeoutPolicy);
  return policy;
}

function parseDurationLiteral(node, context) {
  if (t.isNumericLiteral(node) && Number.isInteger(node.value)) {
    return node.value;
  }

  if (!t.isStringLiteral(node)) {
    throw buildCodeFrameError(context.resourcePath, node, 'Duration values must be integer seconds or strings like "10m"');
  }

  const match = /^(\d+)(s|m|h)$/.exec(node.value.trim());
  if (!match) {
    throw buildCodeFrameError(context.resourcePath, node, `Unsupported duration literal: ${node.value}`);
  }

  const value = Number.parseInt(match[1], 10);
  const unit = match[2];
  if (unit === 's') {
    return value;
  }
  if (unit === 'm') {
    return value * 60;
  }
  if (unit === 'h') {
    return value * 60 * 60;
  }

  throw buildCodeFrameError(context.resourcePath, node, `Unsupported duration literal: ${node.value}`);
}

function rewriteWorkflowClass(classNode, runMethod, compiled) {
  const metadataProperty = t.classProperty(
    t.identifier('__waymarkCompiledWorkflow'),
    t.objectExpression([
      t.objectProperty(t.identifier('workflowName'), t.stringLiteral(compiled.workflowName)),
      t.objectProperty(t.identifier('workflowVersion'), t.stringLiteral(compiled.workflowVersion)),
      t.objectProperty(t.identifier('irHash'), t.stringLiteral(compiled.irHash)),
      t.objectProperty(t.identifier('programBase64'), t.stringLiteral(compiled.programBase64)),
      t.objectProperty(
        t.identifier('inputNames'),
        t.arrayExpression(compiled.inputNames.map((name) => t.stringLiteral(name)))
      ),
      t.objectProperty(t.identifier('concurrent'), t.booleanLiteral(compiled.concurrent))
    ])
  );
  metadataProperty.static = true;

  const valueIdentifiers = compiled.params.map((param) => {
    if (t.isIdentifier(param)) {
      return t.identifier(param.name);
    }
    if (t.isAssignmentPattern(param) && t.isIdentifier(param.left)) {
      return t.identifier(param.left.name);
    }
    throw new Error('Unsupported parameter shape while rewriting workflow');
  });

  const rewrittenRun = t.classMethod(
    'method',
    t.identifier('run'),
    compiled.params.map((param) => t.cloneNode(param, true)),
    t.blockStatement([
      t.returnStatement(
        t.awaitExpression(
          t.callExpression(t.identifier('__waymarkRunCompiled'), [
            t.memberExpression(t.thisExpression(), t.identifier('constructor')),
            t.arrayExpression(valueIdentifiers)
          ])
        )
      )
    ])
  );
  rewrittenRun.async = true;
  rewrittenRun.returnType = runMethod.returnType || null;

  classNode.body.body = [
    metadataProperty,
    ...classNode.body.body.map((member) => (member === runMethod ? rewrittenRun : member))
  ];
}

function ensureWaymarkImports(ast, helperNames) {
  if (helperNames.length === 0) {
    return;
  }

  for (const node of ast.program.body) {
    if (!t.isImportDeclaration(node) || node.source.value !== '@waymark/nextjs') {
      continue;
    }

    for (const helperName of helperNames) {
      const hasHelper = node.specifiers.some(
        (specifier) =>
          t.isImportSpecifier(specifier) &&
          t.isIdentifier(specifier.imported) &&
          specifier.imported.name === helperName
      );
      if (hasHelper) {
        continue;
      }
      node.specifiers.push(
        t.importSpecifier(
          t.identifier(helperName),
          t.identifier(helperName)
        )
      );
    }
    return;
  }

  ast.program.body.unshift(
    t.importDeclaration(
      helperNames.map((helperName) =>
        t.importSpecifier(t.identifier(helperName), t.identifier(helperName))
      ),
      t.stringLiteral('@waymark/nextjs')
    )
  );
}

function injectActionRegistrations(ast, localActions) {
  for (const actionMetadata of localActions.values()) {
    ast.program.body.push(
      t.expressionStatement(
        t.callExpression(t.identifier('__waymarkRegisterAction'), [
          t.stringLiteral(actionMetadata.moduleName),
          t.stringLiteral(actionMetadata.actionName),
          t.identifier(actionMetadata.actionName),
          t.arrayExpression(
            actionMetadata.paramNames.map((paramName) => t.stringLiteral(paramName))
          )
        ])
      )
    );
  }
}

function extractParamNames(params, resourcePath, node) {
  return params.map((param) => {
    if (t.isIdentifier(param)) {
      return param.name;
    }

    if (t.isAssignmentPattern(param) && t.isIdentifier(param.left)) {
      return param.left.name;
    }

    throw buildCodeFrameError(resourcePath, node, 'Only identifier parameters are supported');
  });
}

function extractForLoopVariable(node, resourcePath) {
  if (t.isIdentifier(node)) {
    return node.name;
  }
  if (
    t.isVariableDeclaration(node) &&
    node.declarations.length === 1 &&
    t.isIdentifier(node.declarations[0].id)
  ) {
    return node.declarations[0].id.name;
  }

  throw buildCodeFrameError(resourcePath, node, 'for...of loops require a single identifier');
}

function extractReturnedExpression(body) {
  if (!t.isBlockStatement(body)) {
    return body;
  }

  if (body.body.length !== 1 || !t.isReturnStatement(body.body[0]) || !body.body[0].argument) {
    throw new Error('Callback bodies must be a single return statement');
  }

  return body.body[0].argument;
}

function asBlockStatements(node) {
  if (t.isBlockStatement(node)) {
    return node.body;
  }
  return [node];
}

function unwrapAwait(node) {
  if (t.isAwaitExpression(node)) {
    return unwrapAwait(node.argument);
  }
  return node;
}

function hasUseActionComment(exportNode, declarationNode) {
  const comments = [
    ...(exportNode.leadingComments || []),
    ...(declarationNode.leadingComments || [])
  ];
  return comments.some((comment) => comment.value.trim() === 'use action');
}

function moduleNameForCurrentFile(resourcePath, projectRoot) {
  return path.relative(projectRoot, resourcePath).split(path.sep).join('/');
}

function nextSyntheticName(context, prefix) {
  const index = context.tempState.nextSyntheticNameIndex;
  context.tempState.nextSyntheticNameIndex += 1;
  return `__waymark_${prefix}_${index}`;
}

function isThisRunActionCall(node) {
  return (
    t.isCallExpression(node) &&
    t.isMemberExpression(node.callee) &&
    t.isThisExpression(node.callee.object) &&
    t.isIdentifier(node.callee.property) &&
    node.callee.property.name === 'runAction'
  );
}

function isPromiseAllCall(node) {
  return (
    t.isCallExpression(node) &&
    t.isMemberExpression(node.callee) &&
    !node.callee.computed &&
    t.isIdentifier(node.callee.object) &&
    node.callee.object.name === 'Promise' &&
    t.isIdentifier(node.callee.property) &&
    node.callee.property.name === 'all'
  );
}

function isArrayMapCall(node) {
  return (
    t.isCallExpression(node) &&
    t.isMemberExpression(node.callee) &&
    !node.callee.computed &&
    t.isIdentifier(node.callee.property) &&
    node.callee.property.name === 'map' &&
    node.arguments.length === 1
  );
}

function objectKeyToExpr(node) {
  if (t.isIdentifier(node)) {
    return literalExpr((literal) => literal.setStringValue(node.name));
  }
  if (t.isStringLiteral(node)) {
    return literalExpr((literal) => literal.setStringValue(node.value));
  }
  if (t.isNumericLiteral(node)) {
    if (Number.isInteger(node.value)) {
      return literalExpr((literal) => literal.setIntValue(node.value));
    }
    return literalExpr((literal) => literal.setFloatValue(node.value));
  }
  throw new Error(`Unsupported object key type: ${node.type}`);
}

function objectKeyName(node) {
  if (t.isIdentifier(node)) {
    return node.name;
  }
  if (t.isStringLiteral(node)) {
    return node.value;
  }
  throw new Error(`Unsupported object key type: ${node.type}`);
}

function binaryOperatorFor(operator, resourcePath, node) {
  switch (operator) {
    case '+':
      return ir.BinaryOperator.BINARY_OP_ADD;
    case '-':
      return ir.BinaryOperator.BINARY_OP_SUB;
    case '*':
      return ir.BinaryOperator.BINARY_OP_MUL;
    case '/':
      return ir.BinaryOperator.BINARY_OP_DIV;
    case '%':
      return ir.BinaryOperator.BINARY_OP_MOD;
    case '===':
    case '==':
      return ir.BinaryOperator.BINARY_OP_EQ;
    case '!==':
    case '!=':
      return ir.BinaryOperator.BINARY_OP_NE;
    case '<':
      return ir.BinaryOperator.BINARY_OP_LT;
    case '<=':
      return ir.BinaryOperator.BINARY_OP_LE;
    case '>':
      return ir.BinaryOperator.BINARY_OP_GT;
    case '>=':
      return ir.BinaryOperator.BINARY_OP_GE;
    case '&&':
      return ir.BinaryOperator.BINARY_OP_AND;
    case '||':
      return ir.BinaryOperator.BINARY_OP_OR;
    default:
      throw buildCodeFrameError(resourcePath, node, `Unsupported binary operator: ${operator}`);
  }
}

function variableExpr(name) {
  const variable = new ir.Variable();
  variable.setName(name);
  const expr = new ir.Expr();
  expr.setVariable(variable);
  return expr;
}

function literalExpr(setter) {
  const literal = new ir.Literal();
  setter(literal);
  const expr = new ir.Expr();
  expr.setLiteral(literal);
  return expr;
}

function buildCodeFrameError(resourcePath, node, message) {
  const location = node.loc && node.loc.start
    ? `${resourcePath}:${node.loc.start.line}:${node.loc.start.column + 1}`
    : resourcePath;
  return new Error(`${location} ${message}`);
}

module.exports = {
  collectImportedActionBindings,
  collectLocalActionExports,
  compileWorkflowClass,
  loadActionExports,
  resolveImportPath,
  transformSource
};
