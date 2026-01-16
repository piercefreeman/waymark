import path from "node:path";
import { parse } from "@babel/parser";

import { buildProgram } from "./ir-builder.js";
import { encodeProgram } from "./proto.js";

const WORKFLOW_IMPORT = "@rappel/js/workflow";
const ACTION_IMPORT = "@rappel/js/action";

type CompileMode = "workflow" | "action";

export async function compileSource(
  source,
  filename = "unknown",
  options: { mode?: CompileMode } = {}
) {
  const mode = options.mode ?? "workflow";
  const ast = parseSource(source);
  const actionDefinitions = collectActionDefinitions(ast, filename);
  const actionBindings = collectActionBindings(ast, filename, actionDefinitions);
  const workflows = collectWorkflows(ast, filename);

  if (workflows.length === 0 && actionDefinitions.length === 0) {
    return { code: source, workflows: [] };
  }

  const compiled = [];
  if (mode === "workflow") {
    for (const workflow of workflows) {
      const program = buildProgram({
        functionNode: workflow.node,
        functionName: workflow.shortName,
        actionBindings,
        sourcePath: workflow.sourcePath,
      });
      const irBytes = await encodeProgram(program);
      const irBase64 = Buffer.from(irBytes).toString("base64");
      compiled.push({
        ...workflow,
        irBase64,
        irHash: workflow.irHash,
      });
    }
  }

  const registrationSnippet = buildRegistrationSnippet(
    compiled,
    actionDefinitions,
    mode
  );
  const rewrittenSource =
    mode === "workflow"
      ? rewriteActionBodies(source, actionDefinitions)
      : source;
  const manifestComment = buildManifestComment(workflows, actionDefinitions);
  const updatedSource = injectRegistration(
    rewrittenSource,
    ast,
    registrationSnippet,
    {
      needsWorkflowRegister: mode === "workflow" && workflows.length > 0,
      needsActionRegister: mode === "action" && actionDefinitions.length > 0,
      needsActionProxy: mode === "workflow" && actionDefinitions.length > 0,
    },
    manifestComment
  );

  return {
    code: updatedSource,
    workflows: compiled,
  };
}

function parseSource(source) {
  return parse(source, {
    sourceType: "module",
    plugins: ["typescript", "jsx", "topLevelAwait"],
  });
}

function collectActionDefinitions(ast, filename) {
  const actions = [];
  const seen = new Set();
  for (const node of ast.program.body) {
    const declaration = unwrapExport(node);
    if (!declaration) {
      continue;
    }
    if (declaration.type === "FunctionDeclaration") {
      const action = parseAction(declaration, declaration.id?.name, filename);
      if (action) {
        if (seen.has(action.actionName)) {
          throw errorAt(
            node,
            filename,
            `action '${action.actionName}' already defined`
          );
        }
        seen.add(action.actionName);
        actions.push(action);
      }
      continue;
    }
    if (declaration.type === "VariableDeclaration") {
      for (const declarator of declaration.declarations) {
        if (declarator.id.type !== "Identifier") {
          continue;
        }
        const init = declarator.init;
        if (!init) {
          continue;
        }
        if (
          init.type === "ArrowFunctionExpression" ||
          init.type === "FunctionExpression"
        ) {
          const action = parseAction(init, declarator.id.name, filename);
          if (action) {
            if (seen.has(action.actionName)) {
              throw errorAt(
                declarator,
                filename,
                `action '${action.actionName}' already defined`
              );
            }
            seen.add(action.actionName);
            actions.push(action);
          }
        }
      }
    }
  }
  return actions;
}

function collectActionBindings(ast, filename, actionDefinitions) {
  const bindings = new Map();

  for (const actionDef of actionDefinitions) {
    bindings.set(actionDef.actionName, {
      moduleName: actionDef.moduleName,
      actionName: actionDef.actionName,
      params: actionDef.params,
    });
  }

  for (const node of ast.program.body) {
    const declaration = unwrapExport(node);
    if (!declaration) {
      continue;
    }
    if (declaration.type === "VariableDeclaration") {
      for (const declarator of declaration.declarations) {
        if (
          declarator.id.type !== "Identifier" ||
          !declarator.init ||
          declarator.init.type !== "CallExpression"
        ) {
          continue;
        }
        const call = declarator.init;
        if (call.callee.type !== "Identifier" || call.callee.name !== "action") {
          continue;
        }
        if (call.arguments.length < 2) {
          throw errorAt(
            declarator,
            filename,
            "action requires moduleName and actionName"
          );
        }
        const moduleArg = call.arguments[0];
        const nameArg = call.arguments[1];
        if (
          moduleArg.type !== "StringLiteral" ||
          nameArg.type !== "StringLiteral"
        ) {
          throw errorAt(
            declarator,
            filename,
            "action moduleName and actionName must be string literals"
          );
        }
        const existing = bindings.get(declarator.id.name);
        if (existing) {
          throw errorAt(
            declarator,
            filename,
            `action '${declarator.id.name}' already defined`
          );
        }
        bindings.set(declarator.id.name, {
          moduleName: moduleArg.value,
          actionName: nameArg.value,
          params: null,
        });
      }
    }
  }
  return bindings;
}

function collectWorkflows(ast, filename) {
  const workflows = [];
  for (const node of ast.program.body) {
    const declaration = unwrapExport(node);
    if (!declaration) {
      continue;
    }
    if (declaration.type === "FunctionDeclaration") {
      const workflow = parseWorkflow(declaration, declaration.id?.name, filename);
      if (workflow) {
        workflows.push(workflow);
      }
      continue;
    }
    if (declaration.type === "VariableDeclaration") {
      for (const declarator of declaration.declarations) {
        if (declarator.id.type !== "Identifier") {
          continue;
        }
        const init = declarator.init;
        if (!init) {
          continue;
        }
        if (
          init.type === "ArrowFunctionExpression" ||
          init.type === "FunctionExpression"
        ) {
          const workflow = parseWorkflow(init, declarator.id.name, filename);
          if (workflow) {
            workflows.push(workflow);
          }
        }
      }
    }
  }
  return workflows;
}

function parseWorkflow(node, bindingName, filename) {
  if (!node.async) {
    return null;
  }

  if (node.body.type !== "BlockStatement") {
    throw errorAt(node, filename, "workflow must use a block body");
  }

  if (!hasDirective(node.body, "use workflow")) {
    return null;
  }

  const workflowName = bindingName || node.id?.name;
  if (!workflowName) {
    throw errorAt(node, filename, "workflow must have a name");
  }

  const shortName = workflowName.toLowerCase();
  const sourcePath = normalizeSourcePath(filename);
  const id = `workflow//${sourcePath}//${workflowName}`;
  const irHash = null;

  return {
    node,
    bindingName,
    workflowName,
    shortName,
    sourcePath,
    id,
    irHash,
  };
}

function parseAction(node, bindingName, filename) {
  if (!node.async) {
    return null;
  }

  if (node.body.type !== "BlockStatement") {
    throw errorAt(node, filename, "action must use a block body");
  }

  if (!hasDirective(node.body, "use action")) {
    return null;
  }

  const actionName = bindingName || node.id?.name;
  if (!actionName) {
    throw errorAt(node, filename, "action must have a name");
  }

  const sourcePath = normalizeSourcePath(filename);
  const moduleName = moduleNameFromPath(sourcePath);
  const params = extractParamNames(node.params, filename);
  const id = `action//${sourcePath}//${actionName}`;

  return {
    node,
    bindingName: actionName,
    actionName,
    moduleName,
    params,
    sourcePath,
    id,
    bodyStart: node.body.start,
    bodyEnd: node.body.end,
  };
}

function isUseWorkflowDirective(node) {
  if (!node) {
    return false;
  }
  if (node.type === "Directive") {
    return node.value?.value === "use workflow";
  }
  return (
    node.type === "ExpressionStatement" &&
    node.expression.type === "StringLiteral" &&
    node.expression.value === "use workflow"
  );
}

function isUseActionDirective(node) {
  if (!node) {
    return false;
  }
  if (node.type === "Directive") {
    return node.value?.value === "use action";
  }
  return (
    node.type === "ExpressionStatement" &&
    node.expression.type === "StringLiteral" &&
    node.expression.value === "use action"
  );
}

function hasDirective(block, directiveValue) {
  const directives = block?.directives ?? [];
  if (directives.some((directive) => directive?.value?.value === directiveValue)) {
    return true;
  }

  const [firstStatement] = block?.body ?? [];
  if (directiveValue === "use workflow") {
    return isUseWorkflowDirective(firstStatement);
  }
  if (directiveValue === "use action") {
    return isUseActionDirective(firstStatement);
  }
  return false;
}

function unwrapExport(node) {
  if (!node) {
    return null;
  }
  if (node.type === "ExportNamedDeclaration") {
    return node.declaration;
  }
  if (node.type === "ExportDefaultDeclaration") {
    return node.declaration;
  }
  return node;
}

function normalizeSourcePath(filename) {
  const safeName = filename || "workflow.js";
  const relative = path
    .relative(process.cwd(), safeName)
    .replace(/\\/g, "/");
  return relative || path.basename(safeName);
}

function moduleNameFromPath(sourcePath) {
  const withoutExt = sourcePath.replace(/\.[^.]+$/, "");
  return withoutExt.split("/").filter(Boolean).join(".");
}

function extractParamNames(params, filename) {
  const names = [];
  for (const param of params) {
    if (param.type === "Identifier") {
      names.push(param.name);
      continue;
    }
    if (param.type === "AssignmentPattern" && param.left.type === "Identifier") {
      names.push(param.left.name);
      continue;
    }
    throw errorAt(param, filename, "action parameters must be identifiers");
  }
  return names;
}

function rewriteActionBodies(source, actionDefinitions) {
  if (!actionDefinitions.length) {
    return source;
  }

  const replacements = actionDefinitions.map((action) => {
    if (!Number.isInteger(action.bodyStart) || !Number.isInteger(action.bodyEnd)) {
      throw new Error(`unable to locate action body for ${action.actionName}`);
    }
    return {
      start: action.bodyStart,
      end: action.bodyEnd,
      stub: buildActionStub(action),
      action: action.actionName,
    };
  });

  replacements.sort((a, b) => b.start - a.start);

  let updated = source;
  for (const entry of replacements) {
    if (entry.start >= entry.end) {
      throw new Error(`invalid action range for ${entry.action}`);
    }
    updated =
      updated.slice(0, entry.start) + entry.stub + updated.slice(entry.end);
  }

  return updated;
}

function buildActionStub(action) {
  const params = action.params ?? [];
  const kwargs =
    params.length === 0 ? "{}" : `{ ${params.map((name) => name).join(", ")} }`;
  return `{\n  return __rappelActionProxy(${JSON.stringify(
    action.moduleName
  )}, ${JSON.stringify(action.actionName)}, ${kwargs});\n}`;
}

function injectRegistration(
  source,
  ast,
  registrationSnippet,
  options,
  manifestComment
) {
  const shebangEnd = source.startsWith("#!")
    ? source.indexOf("\n") + 1
    : 0;

  const directiveEnd = findDirectiveEnd(ast) || shebangEnd;
  const insertIndex = Math.max(shebangEnd, directiveEnd);

  const importLines = [];
  const actionImports = [];
  if (options?.needsActionRegister) {
    actionImports.push("registerAction as __rappelRegisterAction");
  }
  if (options?.needsActionProxy) {
    actionImports.push("actionProxy as __rappelActionProxy");
  }
  if (actionImports.length > 0) {
    importLines.push(
      `import { ${actionImports.join(", ")} } from "${ACTION_IMPORT}";\n`
    );
  }
  if (options?.needsWorkflowRegister) {
    importLines.push(
      `import { registerWorkflow as __rappelRegisterWorkflow } from "${WORKFLOW_IMPORT}";\n`
    );
  }
  const importBlock = importLines.join("");
  const manifestBlock = manifestComment ? `${manifestComment}\n` : "";
  const trailingRegistration = registrationSnippet
    ? `\n\n${registrationSnippet}`
    : "";

  return (
    source.slice(0, insertIndex) +
    (insertIndex > 0 ? "\n" : "") +
    manifestBlock +
    importBlock +
    source.slice(insertIndex) +
    trailingRegistration
  );
}

function findDirectiveEnd(ast) {
  let end = 0;
  for (const node of ast.program.body) {
    if (
      node.type === "ExpressionStatement" &&
      node.expression.type === "StringLiteral"
    ) {
      end = node.end;
      continue;
    }
    break;
  }
  return end;
}

function buildRegistrationSnippet(workflows, actions, mode: CompileMode) {
  const actionSnippet =
    mode === "action"
      ? actions
          .map((action, index) => {
            const defName = `__rappelActionDef${index}`;
            const registration = {
              id: action.id,
              moduleName: action.moduleName,
              actionName: action.actionName,
              params: action.params,
              sourcePath: action.sourcePath,
            };
            const defLiteral = JSON.stringify(registration, null, 2);
            return `const ${defName} = ${defLiteral};\n__rappelRegisterAction(${defName}, ${action.bindingName});`;
          })
          .join("\n\n")
      : "";

  const workflowSnippet =
    mode === "workflow"
      ? workflows
          .map((workflow, index) => {
            const defName = `__rappelWorkflowDef${index}`;
            const registration = {
              id: workflow.id,
              shortName: workflow.shortName,
              ir: workflow.irBase64,
              irHash: workflow.irHash,
              sourcePath: workflow.sourcePath,
              concurrent: false,
            };
            const defLiteral = JSON.stringify(registration, null, 2);
            const registerCall = workflow.bindingName
              ? `__rappelRegisterWorkflow(${defName}, ${workflow.bindingName});`
              : `__rappelRegisterWorkflow(${defName});`;
            return `const ${defName} = ${defLiteral};\n${registerCall}`;
          })
          .join("\n\n")
      : "";

  return [actionSnippet, workflowSnippet].filter(Boolean).join("\n\n");
}

function buildManifestComment(workflows, actions) {
  if (workflows.length === 0 && actions.length === 0) {
    return "";
  }

  const manifest: Record<string, unknown> = {};
  if (workflows.length > 0) {
    const workflowManifest: Record<string, Record<string, unknown>> = {};
    for (const workflow of workflows) {
      if (!workflowManifest[workflow.sourcePath]) {
        workflowManifest[workflow.sourcePath] = {};
      }
      workflowManifest[workflow.sourcePath][workflow.shortName] = {
        workflowId: workflow.id,
        shortName: workflow.shortName,
      };
    }
    manifest["workflows"] = workflowManifest;
  }

  if (actions.length > 0) {
    const actionManifest: Record<string, Record<string, unknown>> = {};
    for (const action of actions) {
      if (!actionManifest[action.sourcePath]) {
        actionManifest[action.sourcePath] = {};
      }
      actionManifest[action.sourcePath][action.actionName] = {
        actionId: action.id,
        actionName: action.actionName,
        moduleName: action.moduleName,
      };
    }
    manifest["actions"] = actionManifest;
  }

  return `/**__rappel_internal${JSON.stringify(manifest)}*/;`;
}

function errorAt(node, filename, message) {
  const location = node?.loc
    ? `${filename}:${node.loc.start.line}:${node.loc.start.column}`
    : filename;
  return new Error(`${message} (${location})`);
}
