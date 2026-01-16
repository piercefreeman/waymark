const BINARY_OP_MAP = new Map([
  ["+", 1],
  ["-", 2],
  ["*", 3],
  ["/", 4],
  ["%", 6],
  ["==", 10],
  ["===", 10],
  ["!=", 11],
  ["!==", 11],
  ["<", 12],
  ["<=", 13],
  [">", 14],
  [">=", 15],
  ["in", 16],
  ["&&", 20],
  ["||", 21],
]);

const UNARY_OP_MAP = new Map([
  ["-", 1],
  ["!", 2],
]);

const GLOBAL_FUNCTIONS = new Map([
  ["range", 1],
  ["len", 2],
  ["enumerate", 3],
  ["isexception", 4],
  ["isException", 4],
]);

export function buildProgram({
  functionNode,
  functionName,
  actionBindings,
  sourcePath,
}) {
  const inputs = functionNode.params.map((param) =>
    getIdentifierName(param, sourcePath)
  );

  const statements = compileStatements(
    stripWorkflowDirective(functionNode.body),
    actionBindings,
    sourcePath
  );

  return {
    functions: [
      {
        name: functionName,
        io: {
          inputs,
          outputs: [],
        },
        body: {
          statements,
        },
      },
    ],
  };
}

function stripWorkflowDirective(body) {
  if (body?.directives?.length) {
    return body.body;
  }

  const [firstStatement] = body?.body ?? [];
  if (
    firstStatement &&
    firstStatement.type === "ExpressionStatement" &&
    firstStatement.expression.type === "StringLiteral" &&
    firstStatement.expression.value === "use workflow"
  ) {
    return body.body.slice(1);
  }

  return body.body ?? [];
}

function compileStatements(nodes, actionBindings, sourcePath) {
  const statements = [];
  for (const node of nodes) {
    const compiled = compileStatement(node, actionBindings, sourcePath);
    if (compiled.length) {
      statements.push(...compiled);
    }
  }
  return statements;
}

function compileStatement(node, actionBindings, sourcePath) {
  switch (node.type) {
    case "VariableDeclaration":
      return node.declarations.flatMap((decl) =>
        compileDeclarator(decl, actionBindings, sourcePath)
      );
    case "ExpressionStatement":
      return compileExpressionStatement(node, actionBindings, sourcePath);
    case "ReturnStatement":
      if (!node.argument) {
        return [{ return_stmt: {} }];
      }
      if (node.argument.type === "AwaitExpression") {
        return [
          {
            return_stmt: {
              value: {
                action_call: compileAwaitedAction(
                  node.argument,
                  actionBindings,
                  sourcePath
                ),
              },
            },
          },
        ];
      }
      return [
        {
          return_stmt: {
            value: compileExpr(node.argument, actionBindings, sourcePath),
          },
        },
      ];
    case "IfStatement":
      return [
        {
          conditional: compileConditional(node, actionBindings, sourcePath),
        },
      ];
    case "ForOfStatement":
      return [
        {
          for_loop: compileForLoop(node, actionBindings, sourcePath),
        },
      ];
    case "TryStatement":
      return [
        {
          try_except: compileTryExcept(node, actionBindings, sourcePath),
        },
      ];
    case "BreakStatement":
      return [{ break_stmt: {} }];
    case "ContinueStatement":
      return [{ continue_stmt: {} }];
    default:
      throw unsupported(node, sourcePath);
  }
}

function compileDeclarator(node, actionBindings, sourcePath) {
  const targets = extractTargets(node.id, sourcePath);
  if (!node.init) {
    throw errorAt(node, sourcePath, "workflow assignments require an initializer");
  }

  const expr = compileAssignmentValue(node.init, actionBindings, sourcePath);
  return [
    {
      assignment: {
        targets,
        value: expr,
      },
    },
  ];
}

function compileExpressionStatement(node, actionBindings, sourcePath) {
  const expr = node.expression;

  if (expr.type === "AssignmentExpression") {
    if (expr.operator !== "=") {
      throw errorAt(
        expr,
        sourcePath,
        "workflow assignments only support '='"
      );
    }
    const targets = extractTargets(expr.left, sourcePath);
    const value = compileAssignmentValue(expr.right, actionBindings, sourcePath);
    return [
      {
        assignment: {
          targets,
          value,
        },
      },
    ];
  }

  if (expr.type === "AwaitExpression") {
    const actionCall = compileAwaitedAction(expr, actionBindings, sourcePath);
    return [
      {
        action_call: actionCall,
      },
    ];
  }

  return [
    {
      expr_stmt: {
        expr: compileExpr(expr, actionBindings, sourcePath),
      },
    },
  ];
}

function compileAssignmentValue(node, actionBindings, sourcePath) {
  if (node.type === "AwaitExpression") {
    return {
      action_call: compileAwaitedAction(node, actionBindings, sourcePath),
    };
  }
  return compileExpr(node, actionBindings, sourcePath);
}

function compileAwaitedAction(node, actionBindings, sourcePath) {
  if (node.type !== "AwaitExpression") {
    throw errorAt(node, sourcePath, "expected awaited action");
  }
  if (node.argument.type !== "CallExpression") {
    throw errorAt(node, sourcePath, "await must wrap an action call");
  }
  return compileActionWrapper(node.argument, actionBindings, sourcePath);
}

function compileConditional(node, actionBindings, sourcePath) {
  const ifBranch = {
    condition: compileExpr(node.test, actionBindings, sourcePath),
    block_body: {
      statements: compileStatements(
        ensureBlock(node.consequent).body,
        actionBindings,
        sourcePath
      ),
    },
  };

  const elifBranches = [];
  let currentAlternate = node.alternate;
  while (currentAlternate && currentAlternate.type === "IfStatement") {
    elifBranches.push({
      condition: compileExpr(currentAlternate.test, actionBindings, sourcePath),
      block_body: {
        statements: compileStatements(
          ensureBlock(currentAlternate.consequent).body,
          actionBindings,
          sourcePath
        ),
      },
    });
    currentAlternate = currentAlternate.alternate;
  }

  const elseBranch = currentAlternate
    ? {
        block_body: {
          statements: compileStatements(
            ensureBlock(currentAlternate).body,
            actionBindings,
            sourcePath
          ),
        },
      }
    : null;

  return {
    if_branch: ifBranch,
    elif_branches: elifBranches,
    else_branch: elseBranch,
  };
}

function compileForLoop(node, actionBindings, sourcePath) {
  const loopVars = extractTargets(node.left, sourcePath);
  return {
    loop_vars: loopVars,
    iterable: compileExpr(node.right, actionBindings, sourcePath),
    block_body: {
      statements: compileStatements(
        ensureBlock(node.body).body,
        actionBindings,
        sourcePath
      ),
    },
  };
}

function compileTryExcept(node, actionBindings, sourcePath) {
  if (!node.handler) {
    throw errorAt(node, sourcePath, "try statements require a catch block");
  }

  if (node.finalizer) {
    throw errorAt(node, sourcePath, "finally blocks are not supported");
  }

  const handler = node.handler;
  const exceptionVar = handler.param
    ? getIdentifierName(handler.param, sourcePath)
    : null;

  return {
    try_block: {
      statements: compileStatements(
        ensureBlock(node.block).body,
        actionBindings,
        sourcePath
      ),
    },
    handlers: [
      {
        exception_types: [],
        exception_var: exceptionVar,
        block_body: {
          statements: compileStatements(
            ensureBlock(handler.body).body,
            actionBindings,
            sourcePath
          ),
        },
      },
    ],
  };
}

function compileExpr(node, actionBindings, sourcePath) {
  switch (node.type) {
    case "Identifier":
      return { variable: { name: node.name } };
    case "StringLiteral":
      return { literal: { string_value: node.value } };
    case "NumericLiteral":
      return Number.isInteger(node.value)
        ? { literal: { int_value: node.value } }
        : { literal: { float_value: node.value } };
    case "BooleanLiteral":
      return { literal: { bool_value: node.value } };
    case "NullLiteral":
      return { literal: { is_none: true } };
    case "BinaryExpression":
    case "LogicalExpression":
      return compileBinaryExpression(node, actionBindings, sourcePath);
    case "UnaryExpression":
      return compileUnaryExpression(node, actionBindings, sourcePath);
    case "ArrayExpression":
      return {
        list: {
          elements: node.elements.map((el) =>
            compileExpr(el, actionBindings, sourcePath)
          ),
        },
      };
    case "ObjectExpression":
      return {
        dict: {
          entries: node.properties.map((prop) =>
            compileDictEntry(prop, actionBindings, sourcePath)
          ),
        },
      };
    case "MemberExpression":
      return compileMemberExpression(node, actionBindings, sourcePath);
    case "CallExpression":
      return {
        function_call: compileFunctionCall(node, actionBindings, sourcePath),
      };
    default:
      throw unsupported(node, sourcePath);
  }
}

function compileBinaryExpression(node, actionBindings, sourcePath) {
  const op = BINARY_OP_MAP.get(node.operator);
  if (!op) {
    throw errorAt(node, sourcePath, `unsupported binary operator '${node.operator}'`);
  }
  return {
    binary_op: {
      left: compileExpr(node.left, actionBindings, sourcePath),
      op,
      right: compileExpr(node.right, actionBindings, sourcePath),
    },
  };
}

function compileUnaryExpression(node, actionBindings, sourcePath) {
  const op = UNARY_OP_MAP.get(node.operator);
  if (!op) {
    throw errorAt(node, sourcePath, `unsupported unary operator '${node.operator}'`);
  }
  return {
    unary_op: {
      op,
      operand: compileExpr(node.argument, actionBindings, sourcePath),
    },
  };
}

function compileMemberExpression(node, actionBindings, sourcePath) {
  if (node.computed) {
    return {
      index: {
        object: compileExpr(node.object, actionBindings, sourcePath),
        index: compileExpr(node.property, actionBindings, sourcePath),
      },
    };
  }

  if (node.property.type !== "Identifier") {
    throw errorAt(node, sourcePath, "dot access requires an identifier");
  }

  if (node.property.name === "length") {
    const global = GLOBAL_FUNCTIONS.get("len");
    if (!global) {
      throw errorAt(node, sourcePath, "global len() is not registered");
    }

    return {
      function_call: {
        name: "len",
        args: [compileExpr(node.object, actionBindings, sourcePath)],
        kwargs: [],
        global_function: global,
      },
    };
  }

  return {
    dot: {
      object: compileExpr(node.object, actionBindings, sourcePath),
      attribute: node.property.name,
    },
  };
}

function compileFunctionCall(node, _actionBindings, sourcePath) {
  if (node.callee.type !== "Identifier") {
    throw errorAt(node, sourcePath, "only identifier function calls are supported");
  }

  const global = GLOBAL_FUNCTIONS.get(node.callee.name);
  if (!global) {
    throw errorAt(
      node,
      sourcePath,
      `function '${node.callee.name}' is not supported in workflows`
    );
  }

  return {
    name: node.callee.name,
    args: node.arguments.map((arg) =>
      compileExpr(arg, _actionBindings, sourcePath)
    ),
    kwargs: [],
    global_function: global,
  };
}

function compileActionCall(node, actionBindings, sourcePath) {
  if (node.callee.type !== "Identifier") {
    throw errorAt(node, sourcePath, "action call must be an identifier");
  }

  const action = actionBindings.get(node.callee.name);
  if (!action) {
    throw errorAt(
      node,
      sourcePath,
      `action '${node.callee.name}' is not registered`
    );
  }

  const { kwargs, policies } = compileActionArgs(
    node.arguments,
    sourcePath,
    actionBindings,
    action
  );

  return {
    action_name: action.actionName,
    module_name: action.moduleName,
    kwargs,
    policies,
  };
}

function compileActionWrapper(node, actionBindings, sourcePath) {
  if (isRunActionCall(node)) {
    return compileRunAction(node, actionBindings, sourcePath);
  }
  return compileActionCall(node, actionBindings, sourcePath);
}

function isRunActionCall(node) {
  return node?.callee?.type === "Identifier" && node.callee.name === "runAction";
}

function compileRunAction(node, actionBindings, sourcePath) {
  if (node.arguments.length < 1 || node.arguments.length > 2) {
    throw errorAt(
      node,
      sourcePath,
      "runAction expects an action call and optional policy metadata"
    );
  }

  const actionArg = node.arguments[0];
  if (!actionArg || actionArg.type !== "CallExpression") {
    throw errorAt(node, sourcePath, "runAction requires an action call");
  }

  const actionCall = compileActionCall(actionArg, actionBindings, sourcePath);
  if (actionCall.policies.length > 0) {
    throw errorAt(
      actionArg,
      sourcePath,
      "runAction should not include policies inside the action call"
    );
  }

  const policies =
    node.arguments.length === 2
      ? compilePolicies(node.arguments[1], sourcePath)
      : [];
  actionCall.policies = policies;
  return actionCall;
}

function compileActionArgs(args, sourcePath, actionBindings, actionInfo) {
  if (!actionInfo?.params) {
    return compileActionArgsKwargs(args, sourcePath, actionBindings);
  }

  const params = actionInfo.params;
  if (args.length === 0) {
    return { kwargs: [], policies: [] };
  }

  let policies = [];
  let positional = args;
  if (
    args.length === params.length + 1 &&
    isPolicyObject(args[args.length - 1])
  ) {
    policies = compilePolicies(args[args.length - 1], sourcePath);
    positional = args.slice(0, -1);
  }

  if (positional.length > params.length) {
    throw errorAt(
      positional[params.length],
      sourcePath,
      "action call has more arguments than parameters"
    );
  }

  const kwargs = positional.map((arg, index) => ({
    name: params[index],
    value: compileExpr(arg, actionBindings, sourcePath),
  }));

  return { kwargs, policies };
}

function compileActionArgsKwargs(args, sourcePath, actionBindings) {
  if (args.length === 0) {
    return { kwargs: [], policies: [] };
  }

  if (args.length > 2) {
    throw errorAt(
      args[0],
      sourcePath,
      "action calls accept up to two arguments"
    );
  }

  const kwargsArg = args[0];
  if (kwargsArg.type !== "ObjectExpression") {
    throw errorAt(
      kwargsArg,
      sourcePath,
      "action calls require a single object literal argument"
    );
  }

  const kwargs = kwargsArg.properties.map((prop) =>
    compileKwarg(prop, sourcePath, actionBindings)
  );

  const policies =
    args.length === 2 ? compilePolicies(args[1], sourcePath) : [];

  return { kwargs, policies };
}

function compilePolicies(node, sourcePath) {
  if (node.type !== "ObjectExpression") {
    throw errorAt(node, sourcePath, "action policies must be an object literal");
  }

  const policies = [];
  for (const prop of node.properties) {
    if (prop.type !== "ObjectProperty") {
      throw errorAt(prop, sourcePath, "spread in policies is not supported");
    }
    const key = getPropertyKey(prop, sourcePath);
    if (key === "timeout") {
      const timeoutSeconds = extractNumericValue(prop.value, sourcePath);
      policies.push({
        timeout: {
          timeout: {
            seconds: timeoutSeconds,
          },
        },
      });
    } else if (key === "retry") {
      policies.push({
        retry: compileRetryPolicy(prop.value, sourcePath),
      });
    } else {
      throw errorAt(prop, sourcePath, `unknown policy '${key}'`);
    }
  }

  return policies;
}

function isPolicyObject(node) {
  if (!node || node.type !== "ObjectExpression") {
    return false;
  }
  if (node.properties.length === 0) {
    return false;
  }
  return node.properties.every((prop) => {
    if (prop.type !== "ObjectProperty") {
      return false;
    }
    const key = prop.key;
    if (key.type === "Identifier") {
      return key.name === "retry" || key.name === "timeout";
    }
    if (key.type === "StringLiteral") {
      return key.value === "retry" || key.value === "timeout";
    }
    return false;
  });
}

function compileRetryPolicy(node, sourcePath) {
  if (node.type !== "ObjectExpression") {
    throw errorAt(node, sourcePath, "retry policy must be an object literal");
  }

  let maxRetries = null;
  let backoffSeconds = null;
  let exceptionTypes = [];

  for (const prop of node.properties) {
    if (prop.type !== "ObjectProperty") {
      throw errorAt(prop, sourcePath, "spread in retry policy is not supported");
    }
    const key = getPropertyKey(prop, sourcePath);
    if (key === "attempts") {
      maxRetries = extractNumericValue(prop.value, sourcePath);
    } else if (key === "backoffSeconds") {
      backoffSeconds = extractNumericValue(prop.value, sourcePath);
    } else if (key === "exceptionTypes") {
      exceptionTypes = extractStringArray(prop.value, sourcePath);
    } else {
      throw errorAt(prop, sourcePath, `unknown retry policy field '${key}'`);
    }
  }

  if (maxRetries === null) {
    throw errorAt(node, sourcePath, "retry policy requires attempts");
  }

  return {
    exception_types: exceptionTypes,
    max_retries: maxRetries,
    backoff: backoffSeconds !== null ? { seconds: backoffSeconds } : undefined,
  };
}

function compileKwarg(prop, sourcePath, actionBindings) {
  if (prop.type !== "ObjectProperty") {
    throw errorAt(prop, sourcePath, "spread in action args is not supported");
  }
  const name = getPropertyKey(prop, sourcePath);
  const value = prop.shorthand
    ? { variable: { name } }
    : compileExpr(prop.value, actionBindings, sourcePath);
  return {
    name,
    value,
  };
}

function compileDictEntry(prop, actionBindings, sourcePath) {
  if (prop.type !== "ObjectProperty") {
    throw errorAt(prop, sourcePath, "spread in dicts is not supported");
  }
  const keyExpr = prop.computed
    ? compileExpr(prop.key, actionBindings, sourcePath)
    : literalFromKey(prop.key, sourcePath);
  const valueExpr = prop.shorthand
    ? { variable: { name: prop.key.name } }
    : compileExpr(prop.value, actionBindings, sourcePath);
  return {
    key: keyExpr,
    value: valueExpr,
  };
}

function literalFromKey(node, sourcePath) {
  if (node.type === "Identifier") {
    return { literal: { string_value: node.name } };
  }
  if (node.type === "StringLiteral") {
    return { literal: { string_value: node.value } };
  }
  if (node.type === "NumericLiteral") {
    return Number.isInteger(node.value)
      ? { literal: { int_value: node.value } }
      : { literal: { float_value: node.value } };
  }
  if (node.type === "BooleanLiteral") {
    return { literal: { bool_value: node.value } };
  }
  if (node.type === "NullLiteral") {
    return { literal: { is_none: true } };
  }
  throw errorAt(node, sourcePath, "unsupported dict key type");
}

function extractTargets(node, sourcePath) {
  if (node.type === "Identifier") {
    return [node.name];
  }

  if (node.type === "ArrayPattern") {
    return node.elements.map((el) => getIdentifierName(el, sourcePath));
  }

  if (node.type === "VariableDeclaration") {
    if (node.declarations.length !== 1) {
      throw errorAt(node, sourcePath, "loop bindings must declare one variable");
    }
    return extractTargets(node.declarations[0].id, sourcePath);
  }

  throw errorAt(node, sourcePath, "unsupported assignment target");
}

function getIdentifierName(node, sourcePath) {
  if (node?.type === "AssignmentPattern") {
    return getIdentifierName(node.left, sourcePath);
  }
  if (!node || node.type !== "Identifier") {
    throw errorAt(node, sourcePath, "expected identifier");
  }
  return node.name;
}

function ensureBlock(node) {
  if (node.type === "BlockStatement") {
    return node;
  }
  return {
    type: "BlockStatement",
    body: [node],
  };
}

function getPropertyKey(prop, sourcePath) {
  if (prop.key.type === "Identifier") {
    return prop.key.name;
  }
  if (prop.key.type === "StringLiteral") {
    return prop.key.value;
  }
  throw errorAt(prop, sourcePath, "object keys must be identifiers or strings");
}

function extractNumericValue(node, sourcePath) {
  if (node.type !== "NumericLiteral") {
    throw errorAt(node, sourcePath, "expected numeric literal");
  }
  if (node.value < 0) {
    throw errorAt(node, sourcePath, "numeric values must be non-negative");
  }
  return node.value;
}

function extractStringArray(node, sourcePath) {
  if (node.type !== "ArrayExpression") {
    throw errorAt(node, sourcePath, "expected string array");
  }

  return node.elements.map((el) => {
    if (!el || el.type !== "StringLiteral") {
      throw errorAt(node, sourcePath, "exceptionTypes must be string literals");
    }
    return el.value;
  });
}

function errorAt(node, sourcePath, message) {
  const location = node?.loc
    ? `${sourcePath}:${node.loc.start.line}:${node.loc.start.column}`
    : sourcePath;
  return new Error(`${message} (${location})`);
}

function unsupported(node, sourcePath) {
  return errorAt(node, sourcePath, `unsupported workflow syntax: ${node?.type}`);
}
