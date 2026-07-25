import { codeFrameColumns } from "@babel/code-frame";
import { parse } from "@babel/parser";
import type {
  ArrayPattern,
  CallExpression,
  ClassDeclaration,
  Directive,
  Expression,
  FunctionExpression,
  ImportDeclaration,
  Node,
  ObjectExpression,
  ObjectProperty,
  SourceLocation,
  Statement as BabelStatement,
} from "@babel/types";
import { isExpression } from "@babel/types";
import { createHash } from "node:crypto";
import path from "node:path";

import { ActionRuntime } from "./internal/proto/action.js";
import {
  BinaryOperator,
  type ActionCall,
  type Block,
  type Call,
  type Expr,
  GlobalFunction,
  type PolicyBracket,
  type Program as ProgramMessage,
  Program,
  type Span,
  type Statement,
  UnaryOperator,
} from "./internal/proto/ast.js";

export interface ResolvedModule {
  readonly external?: boolean;
  readonly path: string;
  readonly source?: string;
}

export type ModuleResolver = (
  specifier: string,
  importer: string,
) => ResolvedModule | Promise<ResolvedModule>;

export interface CompileWorkflowOptions {
  readonly filePath: string;
  readonly projectRoot: string;
  readonly resolveModule: ModuleResolver;
  readonly source: string;
  readonly workflowName?: string;
}

export interface CompiledWorkflow {
  readonly actions: readonly CompiledActionReference[];
  readonly bytes: Buffer;
  readonly hash: string;
  readonly inputName: string;
  readonly moduleId: string;
  readonly program: ProgramMessage;
  readonly workflowName: string;
}

export interface CompiledActionReference {
  readonly actionName: string;
  readonly moduleName: string;
  readonly parameterNames: readonly string[];
}

type ActionDefinition = CompiledActionReference;

export class WorkflowCompileError extends Error {
  readonly filePath: string;
  readonly span?: Span;

  constructor(
    message: string,
    source: string,
    filePath: string,
    node?: Node,
    point?: { readonly line: number; readonly column: number },
  ) {
    const location = node?.loc;
    const frame =
      location === null || location === undefined
        ? point === undefined
          ? ""
          : `\n${codeFrameColumns(
              source,
              {
                start: { line: point.line, column: point.column + 1 },
              },
              { highlightCode: false, message },
            )}`
        : `\n${codeFrameColumns(
            source,
            {
              start: {
                line: location.start.line,
                column: location.start.column + 1,
              },
              end: {
                line: location.end.line,
                column: location.end.column + 1,
              },
            },
            { highlightCode: false, message },
          )}`;
    super(`${filePath}: ${message}${frame}`);
    this.name = "WorkflowCompileError";
    this.filePath = filePath;
    this.span =
      location === null || location === undefined
        ? point === undefined
          ? undefined
          : {
              startLine: point.line,
              startCol: point.column,
              endLine: point.line,
              endCol: point.column + 1,
            }
        : spanFromLocation(location);
  }
}

function spanFromLocation(location: SourceLocation): Span {
  return {
    startLine: location.start.line,
    startCol: location.start.column,
    endLine: location.end.line,
    endCol: location.end.column,
  };
}

function nodeSpan(node: Node): Span {
  if (node.loc === null || node.loc === undefined) {
    throw new Error(`parser did not attach a source location to ${node.type}`);
  }
  return spanFromLocation(node.loc);
}

function parseModule(source: string, filePath: string) {
  try {
    return parse(source, {
      sourceFilename: filePath,
      sourceType: "module",
      plugins: ["typescript", "jsx"],
    });
  } catch (error) {
    if (error instanceof SyntaxError) {
      const point = (
        error as SyntaxError & {
          loc?: { readonly line: number; readonly column: number };
        }
      ).loc;
      throw new WorkflowCompileError(
        error.message,
        source,
        filePath,
        undefined,
        point,
      );
    }
    throw error;
  }
}

function moduleIdentifier(filePath: string, projectRoot: string): string {
  const relative = path.relative(path.resolve(projectRoot), path.resolve(filePath));
  if (
    relative === ".." ||
    relative.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relative)
  ) {
    throw new Error(`${filePath} is outside project root ${projectRoot}`);
  }
  return relative.split(path.sep).join(path.posix.sep);
}

function importedBinding(
  declaration: ImportDeclaration,
  importedName: string,
): string | undefined {
  for (const specifier of declaration.specifiers) {
    if (
      specifier.type === "ImportSpecifier" &&
      (specifier.imported.type === "Identifier"
        ? specifier.imported.name
        : specifier.imported.value) === importedName
    ) {
      return specifier.local.name;
    }
  }
  return undefined;
}

function waymarkBindings(
  statements: readonly BabelStatement[],
): { actionFactory: string; workflowBase: string } {
  let actionFactory = "action";
  let workflowBase = "Workflow";
  for (const statement of statements) {
    if (
      statement.type === "ImportDeclaration" &&
      statement.source.value === "@waymark/nextjs"
    ) {
      actionFactory = importedBinding(statement, "action") ?? actionFactory;
      workflowBase = importedBinding(statement, "Workflow") ?? workflowBase;
    }
  }
  return { actionFactory, workflowBase };
}

function parameterNames(
  implementation: FunctionExpression,
  source: string,
  filePath: string,
): string[] {
  return implementation.params.map((parameter) => {
    if (parameter.type !== "Identifier") {
      throw new WorkflowCompileError(
        "action parameters must be plain identifiers; defaults, rest, and destructuring are unsupported",
        source,
        filePath,
        parameter,
      );
    }
    return parameter.name;
  });
}

function actionFromDeclaration(
  declaration: BabelStatement,
  actionFactory: string,
  moduleName: string,
  source: string,
  filePath: string,
): ActionDefinition[] {
  if (
    declaration.type !== "ExportNamedDeclaration" ||
    declaration.declaration?.type !== "VariableDeclaration"
  ) {
    return [];
  }

  const definitions: ActionDefinition[] = [];
  for (const variable of declaration.declaration.declarations) {
    if (
      variable.init?.type !== "CallExpression" ||
      variable.init.callee.type !== "Identifier" ||
      variable.init.callee.name !== actionFactory
    ) {
      continue;
    }
    if (
      declaration.declaration.kind !== "const" ||
      variable.id.type !== "Identifier" ||
      variable.init.arguments.length !== 1 ||
      variable.init.arguments[0]?.type !== "FunctionExpression"
    ) {
      throw new WorkflowCompileError(
        "actions must use `export const name = action(async function name(...) {})`",
        source,
        filePath,
        variable,
      );
    }

    const implementation = variable.init.arguments[0];
    if (
      !implementation.async ||
      implementation.id?.name !== variable.id.name
    ) {
      throw new WorkflowCompileError(
        "the action function must be async and have the same name as its export",
        source,
        filePath,
        implementation,
      );
    }
    definitions.push({
      actionName: variable.id.name,
      moduleName,
      parameterNames: parameterNames(implementation, source, filePath),
    });
  }
  return definitions;
}

function exportedActions(
  statements: readonly BabelStatement[],
  source: string,
  filePath: string,
  projectRoot: string,
): Map<string, ActionDefinition> {
  const { actionFactory } = waymarkBindings(statements);
  const moduleName = moduleIdentifier(filePath, projectRoot);
  const actions = new Map<string, ActionDefinition>();
  for (const statement of statements) {
    for (const definition of actionFromDeclaration(
      statement,
      actionFactory,
      moduleName,
      source,
      filePath,
    )) {
      actions.set(definition.actionName, definition);
    }
  }
  return actions;
}

async function knownActions(
  statements: readonly BabelStatement[],
  options: CompileWorkflowOptions,
): Promise<Map<string, ActionDefinition>> {
  const actions = exportedActions(
    statements,
    options.source,
    options.filePath,
    options.projectRoot,
  );

  for (const statement of statements) {
    if (
      statement.type !== "ImportDeclaration" ||
      statement.importKind === "type" ||
      statement.source.value === "@waymark/nextjs"
    ) {
      continue;
    }
    const importedBindings = statement.specifiers.filter(
      (specifier) =>
        specifier.type === "ImportSpecifier" &&
        specifier.importKind !== "type",
    );
    if (importedBindings.length === 0) {
      continue;
    }

    const resolved = await options.resolveModule(
      statement.source.value,
      options.filePath,
    );
    if (resolved.external === true) {
      continue;
    }
    if (resolved.source === undefined) {
      throw new WorkflowCompileError(
        `resolver did not load project module ${statement.source.value}`,
        options.source,
        options.filePath,
        statement,
      );
    }
    const resolvedProgram = parseModule(resolved.source, resolved.path);
    const moduleActions = exportedActions(
      resolvedProgram.program.body,
      resolved.source,
      resolved.path,
      options.projectRoot,
    );
    for (const specifier of importedBindings) {
      if (specifier.type !== "ImportSpecifier") {
        continue;
      }
      const importedName =
        specifier.imported.type === "Identifier"
          ? specifier.imported.name
          : specifier.imported.value;
      const definition = moduleActions.get(importedName);
      if (definition !== undefined) {
        actions.set(specifier.local.name, definition);
      }
    }
  }
  return actions;
}

class Lowerer {
  constructor(
    private readonly actions: ReadonlyMap<string, ActionDefinition>,
    private readonly source: string,
    private readonly filePath: string,
  ) {}

  fail(node: Node, message: string): never {
    throw new WorkflowCompileError(message, this.source, this.filePath, node);
  }

  private requireExpression(
    node: Node | null | undefined,
    message: string,
  ): Expression {
    if (node === null || node === undefined || !isExpression(node)) {
      return this.fail(node ?? ({ type: "Identifier" } as Node), message);
    }
    return node;
  }

  block(statements: readonly BabelStatement[], owner: Node): Block {
    return {
      statements: statements.flatMap((statement) => this.statement(statement)),
      span: nodeSpan(owner),
    };
  }

  statement(node: BabelStatement): Statement[] {
    switch (node.type) {
      case "VariableDeclaration":
        return node.declarations.map((declaration) => {
          if (declaration.init === null || declaration.init === undefined) {
            return this.fail(declaration, "workflow variables must be initialized");
          }
          const targets = this.assignmentTargets(declaration.id);
          const value = this.expression(declaration.init);
          if (declaration.id.type === "ArrayPattern") {
            if (
              value.kind?.$case !== "parallelExpr" ||
              value.kind.value.calls.length !== targets.length
            ) {
              return this.fail(
                declaration,
                "array destructuring is only supported for matching Promise.all action arrays",
              );
            }
          }
          return {
            kind: {
              $case: "assignment",
              value: { targets, value },
            },
            span: nodeSpan(declaration),
          };
        });
      case "ExpressionStatement":
        return [this.expressionStatement(node.expression, node)];
      case "ReturnStatement":
        return [
          {
            kind: {
              $case: "returnStmt",
              value: {
                value:
                  node.argument === null || node.argument === undefined
                    ? undefined
                    : this.expression(node.argument),
              },
            },
            span: nodeSpan(node),
          },
        ];
      case "IfStatement":
        return [
          {
            kind: {
              $case: "conditional",
              value: {
                ifBranch: {
                  condition: this.condition(node.test),
                  span: nodeSpan(node.test),
                  blockBody: this.statementBody(node.consequent),
                },
                elifBranches: [],
                elseBranch:
                  node.alternate === null || node.alternate === undefined
                    ? undefined
                    : {
                        span: nodeSpan(node.alternate),
                        blockBody: this.statementBody(node.alternate),
                      },
              },
            },
            span: nodeSpan(node),
          },
        ];
      case "ForOfStatement":
        if (node.await) {
          return this.fail(node, "`for await` is not supported in workflows");
        }
        return [
          {
            kind: {
              $case: "forLoop",
              value: {
                loopVars: this.forLoopTargets(node.left),
                iterable: this.expression(
                  this.requireExpression(
                    node.right,
                    "`for...of` requires a workflow expression",
                  ),
                ),
                blockBody: this.statementBody(node.body),
              },
            },
            span: nodeSpan(node),
          },
        ];
      case "WhileStatement":
        return [
          {
            kind: {
              $case: "whileLoop",
              value: {
                condition: this.condition(node.test),
                blockBody: this.statementBody(node.body),
              },
            },
            span: nodeSpan(node),
          },
        ];
      case "BreakStatement":
        return [
          {
            kind: { $case: "breakStmt", value: {} },
            span: nodeSpan(node),
          },
        ];
      case "ContinueStatement":
        return [
          {
            kind: { $case: "continueStmt", value: {} },
            span: nodeSpan(node),
          },
        ];
      case "TryStatement":
        if (
          node.handler === null ||
          node.handler === undefined ||
          node.finalizer !== null
        ) {
          return this.fail(
            node,
            "workflows support catch-all `try`/`catch` without `finally`",
          );
        }
        const handler = node.handler;
        if (
          handler.param !== null &&
          handler.param !== undefined &&
          handler.param.type !== "Identifier"
        ) {
          return this.fail(
            handler.param,
            "catch bindings must be plain identifiers",
          );
        }
        return [
          {
            kind: {
              $case: "tryExcept",
              value: {
                tryBlock: this.block(node.block.body, node.block),
                handlers: [
                  {
                    exceptionTypes: [],
                    exceptionVar:
                      handler.param?.type === "Identifier"
                        ? handler.param.name
                        : undefined,
                    span: nodeSpan(handler),
                    blockBody: this.block(
                      handler.body.body,
                      handler.body,
                    ),
                  },
                ],
              },
            },
            span: nodeSpan(node),
          },
        ];
      case "BlockStatement":
        return node.body.flatMap((statement) => this.statement(statement));
      case "EmptyStatement":
        return [];
      default:
        return this.fail(node, `${node.type} is not supported in Workflow.run()`);
    }
  }

  private statementBody(node: BabelStatement): Block {
    return node.type === "BlockStatement"
      ? this.block(node.body, node)
      : this.block([node], node);
  }

  private assignmentTargets(pattern: Node): string[] {
    if (pattern.type === "Identifier") {
      return [pattern.name];
    }
    if (pattern.type === "ArrayPattern") {
      return this.arrayPatternTargets(pattern);
    }
    return this.fail(
      pattern,
      "assignments support identifiers or flat array destructuring",
    );
  }

  private arrayPatternTargets(pattern: ArrayPattern): string[] {
    return pattern.elements.map((element) => {
      if (element?.type !== "Identifier") {
        return this.fail(
          element ?? pattern,
          "array destructuring supports identifier elements only",
        );
      }
      return element.name;
    });
  }

  private forLoopTargets(
    left: Node,
  ): string[] {
    if (left.type === "VariableDeclaration") {
      if (
        left.declarations.length !== 1 ||
        left.declarations[0] === undefined
      ) {
        return this.fail(left, "`for...of` must declare exactly one binding");
      }
      return this.assignmentTargets(left.declarations[0].id);
    }
    return this.assignmentTargets(left);
  }

  private expressionStatement(
    expression: Expression,
    owner: BabelStatement,
  ): Statement {
    if (
      expression.type === "AssignmentExpression" &&
      expression.operator === "=" &&
      expression.left.type === "Identifier"
    ) {
      return {
        kind: {
          $case: "assignment",
          value: {
            targets: [expression.left.name],
            value: this.expression(expression.right),
          },
        },
        span: nodeSpan(owner),
      };
    }
    if (expression.type === "AssignmentExpression") {
      return this.fail(
        expression,
        "workflow assignments may only use `=` with a local identifier",
      );
    }

    const lowered = this.expression(expression);
    if (lowered.kind?.$case === "actionCall") {
      return {
        kind: { $case: "actionCall", value: lowered.kind.value },
        span: nodeSpan(owner),
      };
    }
    if (lowered.kind?.$case === "parallelExpr") {
      return {
        kind: {
          $case: "parallelBlock",
          value: { calls: lowered.kind.value.calls },
        },
        span: nodeSpan(owner),
      };
    }
    if (lowered.kind?.$case === "spreadExpr") {
      return {
        kind: {
          $case: "spreadAction",
          value: lowered.kind.value,
        },
        span: nodeSpan(owner),
      };
    }
    return {
      kind: { $case: "exprStmt", value: { expr: lowered } },
      span: nodeSpan(owner),
    };
  }

  expression(node: Expression): Expr {
    switch (node.type) {
      case "AwaitExpression":
        return this.expression(node.argument);
      case "TSAsExpression":
      case "TSSatisfiesExpression":
      case "TSNonNullExpression":
        return this.expression(node.expression);
      case "Identifier":
        if (node.name === "undefined") {
          return this.fail(node, "`undefined` is not a workflow value; use null");
        }
        return this.expr(node, {
          $case: "variable",
          value: { name: node.name },
        });
      case "NullLiteral":
        return this.literal(node, { $case: "isNone", value: true });
      case "BooleanLiteral":
        return this.literal(node, { $case: "boolValue", value: node.value });
      case "StringLiteral":
        return this.literal(node, { $case: "stringValue", value: node.value });
      case "NumericLiteral":
        if (!Number.isFinite(node.value)) {
          return this.fail(node, "numeric workflow literals must be finite");
        }
        if (Number.isInteger(node.value)) {
          if (!Number.isSafeInteger(node.value)) {
            return this.fail(node, "integer workflow literals must be safe integers");
          }
          return this.literal(node, {
            $case: "intValue",
            value: BigInt(node.value),
          });
        }
        return this.literal(node, { $case: "floatValue", value: node.value });
      case "TemplateLiteral":
        if (node.expressions.length !== 0 || node.quasis.length !== 1) {
          return this.fail(node, "template interpolation is not supported");
        }
        return this.literal(node, {
          $case: "stringValue",
          value: node.quasis[0]?.value.cooked ?? "",
        });
      case "ArrayExpression":
        if (node.elements.some((element) => element === null)) {
          return this.fail(node, "workflow arrays cannot contain holes");
        }
        return this.expr(node, {
          $case: "list",
          value: {
            elements: node.elements.map((element) => {
              if (element === null || element.type === "SpreadElement") {
                return this.fail(node, "array spread is not supported");
              }
              return this.expression(element);
            }),
          },
        });
      case "ObjectExpression":
        return this.objectExpression(node);
      case "MemberExpression":
        if (node.object.type === "Super") {
          return this.fail(node, "`super` is not available in Workflow.run()");
        }
        if (node.computed) {
          if (node.property.type === "PrivateName") {
            return this.fail(node.property, "private fields are not supported");
          }
          return this.expr(node, {
            $case: "index",
            value: {
              object: this.expression(node.object),
              index: this.expression(node.property),
            },
          });
        }
        if (node.property.type !== "Identifier") {
          return this.fail(node.property, "workflow property names must be static");
        }
        if (node.property.name === "length") {
          return this.expr(node, {
            $case: "functionCall",
            value: {
              name: "len",
              args: [this.expression(node.object)],
              kwargs: [],
              globalFunction: GlobalFunction.GLOBAL_FUNCTION_LEN,
            },
          });
        }
        return this.expr(node, {
          $case: "index",
          value: {
            object: this.expression(node.object),
            index: this.literal(node.property, {
              $case: "stringValue",
              value: node.property.name,
            }),
          },
        });
      case "BinaryExpression":
        return this.binaryExpression(node);
      case "LogicalExpression":
        return this.expr(node, {
          $case: "binaryOp",
          value: {
            left: this.condition(node.left),
            op:
              node.operator === "&&"
                ? BinaryOperator.BINARY_OP_AND
                : BinaryOperator.BINARY_OP_OR,
            right: this.condition(node.right),
          },
        });
      case "UnaryExpression":
        if (node.operator !== "!" && node.operator !== "-") {
          return this.fail(node, `unary ${node.operator} is not supported`);
        }
        return this.expr(node, {
          $case: "unaryOp",
          value: {
            op:
              node.operator === "!"
                ? UnaryOperator.UNARY_OP_NOT
                : UnaryOperator.UNARY_OP_NEG,
            operand:
              node.operator === "!"
                ? this.condition(node.argument)
                : this.expression(node.argument),
          },
        });
      case "CallExpression":
        return this.callExpression(node);
      case "AssignmentExpression":
        return this.fail(
          node,
          "assignments are statements and may only target a local identifier",
        );
      default:
        return this.fail(node, `${node.type} is not supported in workflow expressions`);
    }
  }

  private expr(node: Node, kind: NonNullable<Expr["kind"]>): Expr {
    return { kind, span: nodeSpan(node) };
  }

  private literal(
    node: Node,
    value: NonNullable<
      Extract<NonNullable<Expr["kind"]>, { $case: "literal" }>["value"]["value"]
    >,
  ): Expr {
    return this.expr(node, { $case: "literal", value: { value } });
  }

  private objectExpression(node: ObjectExpression): Expr {
    return this.expr(node, {
      $case: "dict",
      value: {
        entries: node.properties.map((property) => {
          if (
            property.type !== "ObjectProperty" ||
            property.computed ||
            property.value.type === "AssignmentPattern" ||
            property.value.type === "RestElement" ||
            !isExpression(property.value)
          ) {
            return this.fail(
              property,
              "workflow objects support static data properties only",
            );
          }
          return {
            key: this.objectKey(property),
            value: this.expression(property.value),
          };
        }),
      },
    });
  }

  private objectKey(property: ObjectProperty): Expr {
    const key = property.key;
    if (key.type === "Identifier") {
      return this.literal(key, { $case: "stringValue", value: key.name });
    }
    if (key.type === "StringLiteral") {
      return this.literal(key, { $case: "stringValue", value: key.value });
    }
    if (key.type === "NumericLiteral" && Number.isSafeInteger(key.value)) {
      return this.literal(key, {
        $case: "stringValue",
        value: String(key.value),
      });
    }
    return this.fail(key, "workflow object keys must be static strings");
  }

  private binaryExpression(
    node: import("@babel/types").BinaryExpression,
  ): Expr {
    const operators: Partial<Record<typeof node.operator, BinaryOperator>> = {
      "+": BinaryOperator.BINARY_OP_ADD,
      "-": BinaryOperator.BINARY_OP_SUB,
      "*": BinaryOperator.BINARY_OP_MUL,
      "/": BinaryOperator.BINARY_OP_DIV,
      "%": BinaryOperator.BINARY_OP_MOD,
      "===": BinaryOperator.BINARY_OP_EQ,
      "!==": BinaryOperator.BINARY_OP_NE,
      "<": BinaryOperator.BINARY_OP_LT,
      "<=": BinaryOperator.BINARY_OP_LE,
      ">": BinaryOperator.BINARY_OP_GT,
      ">=": BinaryOperator.BINARY_OP_GE,
    };
    const operator = operators[node.operator];
    if (operator === undefined) {
      return this.fail(
        node,
        `binary ${node.operator} is unsupported; use strict comparisons and explicit arithmetic`,
      );
    }
    if (node.left.type === "PrivateName") {
      return this.fail(node.left, "private fields are not supported");
    }
    return this.expr(node, {
      $case: "binaryOp",
      value: {
        left: this.expression(node.left),
        op: operator,
        right: this.expression(node.right),
      },
    });
  }

  private condition(node: Expression): Expr {
    if (
      node.type !== "BooleanLiteral" &&
      node.type !== "BinaryExpression" &&
      node.type !== "LogicalExpression" &&
      !(node.type === "UnaryExpression" && node.operator === "!")
    ) {
      return this.fail(
        node,
        "workflow conditions must be explicit boolean expressions; implicit truthiness is unsupported",
      );
    }
    if (
      node.type === "BinaryExpression" &&
      !["===", "!==", "<", "<=", ">", ">="].includes(node.operator)
    ) {
      return this.fail(node, "workflow conditions must use explicit comparisons");
    }
    return this.expression(node);
  }

  private callExpression(node: CallExpression): Expr {
    if (node.callee.type === "Identifier") {
      const action = this.actions.get(node.callee.name);
      if (action !== undefined) {
        return this.expr(node, {
          $case: "actionCall",
          value: this.actionCall(node, action, []),
        });
      }
      return this.fail(
        node,
        `call to ${node.callee.name} is unsupported; Workflow.run() may only call declared actions`,
      );
    }
    if (
      node.callee.type === "MemberExpression" &&
      !node.callee.computed &&
      node.callee.object.type === "ThisExpression" &&
      node.callee.property.type === "Identifier" &&
      node.callee.property.name === "runAction"
    ) {
      return this.runAction(node);
    }
    if (this.isPromiseAll(node)) {
      return this.promiseAll(node);
    }
    return this.fail(
      node,
      "arbitrary calls are not supported in Workflow.run(); move the code into an action",
    );
  }

  private actionCall(
    node: CallExpression,
    definition: ActionDefinition,
    policies: PolicyBracket[],
  ): ActionCall {
    if (
      node.arguments.some(
        (argument) =>
          argument.type === "SpreadElement" ||
          argument.type === "ArgumentPlaceholder",
      )
    ) {
      return this.fail(node, "action calls do not support spread arguments");
    }
    if (node.arguments.length !== definition.parameterNames.length) {
      return this.fail(
        node,
        `${definition.actionName} expects ${definition.parameterNames.length} arguments, received ${node.arguments.length}`,
      );
    }
    return {
      actionName: definition.actionName,
      kwargs: definition.parameterNames.map((name, index) => {
        const argument = node.arguments[index];
        if (
          argument === undefined ||
          argument.type === "SpreadElement" ||
          argument.type === "ArgumentPlaceholder"
        ) {
          return this.fail(node, "action calls require positional expressions");
        }
        return { name, value: this.expression(argument) };
      }),
      policies,
      moduleName: definition.moduleName,
      runtime: ActionRuntime.ACTION_RUNTIME_JAVASCRIPT,
    };
  }

  private runAction(node: CallExpression): Expr {
    if (node.arguments.length < 1 || node.arguments.length > 2) {
      return this.fail(
        node,
        "this.runAction expects an action call and optional literal policies",
      );
    }
    const call = node.arguments[0];
    if (call?.type !== "CallExpression" || call.callee.type !== "Identifier") {
      return this.fail(node, "this.runAction must wrap a direct action call");
    }
    const definition = this.actions.get(call.callee.name);
    if (definition === undefined) {
      return this.fail(call, `${call.callee.name} is not a declared action`);
    }
    const policies =
      node.arguments.length === 2
        ? this.policies(node.arguments[1])
        : [];
    return this.expr(node, {
      $case: "actionCall",
      value: this.actionCall(call, definition, policies),
    });
  }

  private policies(
    node:
      | import("@babel/types").Expression
      | import("@babel/types").SpreadElement
      | import("@babel/types").JSXNamespacedName
      | import("@babel/types").ArgumentPlaceholder
      | undefined,
  ): PolicyBracket[] {
    if (node?.type !== "ObjectExpression") {
      return this.fail(
        node ?? ({ type: "ObjectExpression" } as ObjectExpression),
        "action policies must be an object literal",
      );
    }
    const policies: PolicyBracket[] = [];
    for (const property of node.properties) {
      if (
        property.type !== "ObjectProperty" ||
        property.computed ||
        property.key.type !== "Identifier"
      ) {
        return this.fail(property, "action policies must use static properties");
      }
      if (property.key.name === "timeout") {
        if (
          property.value.type !== "StringLiteral" ||
          !/^\d+[smh]$/.test(property.value.value)
        ) {
          return this.fail(
            property.value,
            "timeout must be a literal duration such as \"30s\", \"2m\", or \"1h\"",
          );
        }
        policies.push({
          kind: {
            $case: "timeout",
            value: {
              timeout: {
                seconds: this.durationSeconds(property.value.value),
              },
            },
          },
        });
      } else if (property.key.name === "retry") {
        if (!isExpression(property.value)) {
          return this.fail(property.value, "retry must be an object literal");
        }
        policies.push({
          kind: {
            $case: "retry",
            value: this.retryPolicy(property.value),
          },
        });
      } else {
        return this.fail(property.key, `unknown action policy ${property.key.name}`);
      }
    }
    return policies;
  }

  private retryPolicy(
    node: Expression,
  ): NonNullable<
    Extract<NonNullable<PolicyBracket["kind"]>, { $case: "retry" }>["value"]
  > {
    if (node.type !== "ObjectExpression") {
      return this.fail(node, "retry must be an object literal");
    }
    let attempts: number | undefined;
    let backoffSeconds = 0;
    for (const property of node.properties) {
      if (
        property.type !== "ObjectProperty" ||
        property.computed ||
        property.key.type !== "Identifier" ||
        property.value.type !== "NumericLiteral" ||
        !Number.isSafeInteger(property.value.value)
      ) {
        return this.fail(
          property,
          "retry properties must be static integer literals",
        );
      }
      if (property.key.name === "attempts") {
        attempts = property.value.value;
      } else if (property.key.name === "backoffSeconds") {
        backoffSeconds = property.value.value;
      } else {
        return this.fail(property.key, `unknown retry policy ${property.key.name}`);
      }
    }
    if (attempts === undefined || attempts < 1) {
      return this.fail(node, "retry.attempts must be a positive integer");
    }
    if (backoffSeconds < 0) {
      return this.fail(node, "retry.backoffSeconds cannot be negative");
    }
    return {
      exceptionTypes: [],
      maxRetries: attempts - 1,
      backoff: { seconds: BigInt(backoffSeconds) },
    };
  }

  private durationSeconds(value: string): bigint {
    const amount = BigInt(value.slice(0, -1));
    switch (value.at(-1)) {
      case "s":
        return amount;
      case "m":
        return amount * 60n;
      case "h":
        return amount * 3_600n;
      default:
        throw new Error(`unvalidated duration ${value}`);
    }
  }

  private isPromiseAll(node: CallExpression): boolean {
    return (
      node.callee.type === "MemberExpression" &&
      !node.callee.computed &&
      node.callee.object.type === "Identifier" &&
      node.callee.object.name === "Promise" &&
      node.callee.property.type === "Identifier" &&
      node.callee.property.name === "all"
    );
  }

  private promiseAll(node: CallExpression): Expr {
    if (node.arguments.length !== 1) {
      return this.fail(node, "Promise.all expects exactly one workflow collection");
    }
    const argument = node.arguments[0];
    if (argument?.type === "ArrayExpression") {
      const calls = argument.elements.map((element) => {
        if (element?.type !== "CallExpression") {
          return this.fail(
            element ?? argument,
            "Promise.all arrays may only contain direct action calls",
          );
        }
        return this.parallelCall(element);
      });
      return this.expr(node, {
        $case: "parallelExpr",
        value: { calls },
      });
    }
    if (
      argument?.type === "CallExpression" &&
      argument.callee.type === "MemberExpression" &&
      !argument.callee.computed &&
      argument.callee.property.type === "Identifier" &&
      argument.callee.property.name === "map" &&
      argument.callee.object.type !== "Super" &&
      argument.arguments.length === 1 &&
      argument.arguments[0]?.type === "ArrowFunctionExpression"
    ) {
      const mapper = argument.arguments[0];
      if (
        mapper.params.length !== 1 ||
        mapper.params[0]?.type !== "Identifier" ||
        mapper.body.type !== "CallExpression" ||
        mapper.async
      ) {
        return this.fail(
          mapper,
          "Promise.all map requires `collection.map(item => action(item))`",
        );
      }
      const call = this.parallelCall(mapper.body);
      if (call.kind?.$case !== "action") {
        return this.fail(mapper.body, "Promise.all map must call an action");
      }
      return this.expr(node, {
        $case: "spreadExpr",
        value: {
          collection: this.expression(argument.callee.object),
          loopVar: mapper.params[0].name,
          action: call.kind.value,
        },
      });
    }
    return this.fail(
      argument ?? node,
      "Promise.all supports an action array or collection.map(item => action(item))",
    );
  }

  private parallelCall(node: CallExpression): Call {
    const expression = this.callExpression(node);
    if (expression.kind?.$case !== "actionCall") {
      return this.fail(node, "parallel workflows may only call actions");
    }
    return {
      kind: { $case: "action", value: expression.kind.value },
    };
  }
}

function exportedWorkflowClasses(
  statements: readonly BabelStatement[],
  workflowBase: string,
): ClassDeclaration[] {
  const classes: ClassDeclaration[] = [];
  for (const statement of statements) {
    if (
      statement.type === "ExportNamedDeclaration" &&
      statement.declaration?.type === "ClassDeclaration" &&
      statement.declaration.superClass?.type === "Identifier" &&
      statement.declaration.superClass.name === workflowBase
    ) {
      classes.push(statement.declaration);
    }
  }
  return classes;
}

function workflowRun(
  workflow: ClassDeclaration,
  source: string,
  filePath: string,
) {
  const methods = workflow.body.body.filter(
    (member) =>
      member.type === "ClassMethod" &&
      member.kind === "method" &&
      !member.computed &&
      member.key.type === "Identifier" &&
      member.key.name === "run",
  );
  const method = methods[0];
  if (
    methods.length !== 1 ||
    method?.type !== "ClassMethod" ||
    !method.async
  ) {
    throw new WorkflowCompileError(
      "a workflow must define exactly one async run(...) method",
      source,
      filePath,
      workflow,
    );
  }
  for (const parameter of method.params) {
    if (parameter.type !== "Identifier") {
      throw new WorkflowCompileError(
        "Workflow.run parameters must be plain identifiers",
        source,
        filePath,
        parameter,
      );
    }
  }
  return method;
}

function rejectClientOrEdge(
  statements: readonly BabelStatement[],
  directives: readonly Directive[],
  source: string,
  filePath: string,
): void {
  const clientDirective = directives.find(
    (directive) => directive.value.value === "use client",
  );
  if (clientDirective !== undefined) {
    throw new WorkflowCompileError(
      "Waymark workflows must be declared in server modules, not client components",
      source,
      filePath,
      clientDirective,
    );
  }
  for (const statement of statements) {
    if (
      statement.type === "ExportNamedDeclaration" &&
      statement.declaration?.type === "VariableDeclaration"
    ) {
      for (const declaration of statement.declaration.declarations) {
        if (
          declaration.id.type === "Identifier" &&
          declaration.id.name === "runtime" &&
          declaration.init?.type === "StringLiteral" &&
          declaration.init.value === "edge"
        ) {
          throw new WorkflowCompileError(
            "Waymark workflows require the Next.js Node.js runtime",
            source,
            filePath,
            declaration,
          );
        }
      }
    }
  }
}

export async function compileWorkflow(
  options: CompileWorkflowOptions,
): Promise<CompiledWorkflow> {
  const parsed = parseModule(options.source, options.filePath);
  rejectClientOrEdge(
    parsed.program.body,
    parsed.program.directives,
    options.source,
    options.filePath,
  );
  const { workflowBase } = waymarkBindings(parsed.program.body);
  const workflows = exportedWorkflowClasses(
    parsed.program.body,
    workflowBase,
  );
  const workflow =
    options.workflowName === undefined
      ? workflows.length === 1
        ? workflows[0]
        : undefined
      : workflows.find((candidate) => candidate.id?.name === options.workflowName);
  if (
    workflow === undefined ||
    workflow.id === null ||
    workflow.id === undefined
  ) {
    throw new WorkflowCompileError(
      options.workflowName === undefined
        ? "the module must export exactly one Workflow class"
        : `exported Workflow ${options.workflowName} was not found`,
      options.source,
      options.filePath,
    );
  }
  const workflowName = workflow.id.name;

  const run = workflowRun(workflow, options.source, options.filePath);
  if (run.params.length !== 1 || run.params[0]?.type !== "Identifier") {
    throw new WorkflowCompileError(
      "Workflow.run() must accept exactly one input parameter",
      options.source,
      options.filePath,
      run,
    );
  }
  const inputName = run.params[0].name;
  const actions = await knownActions(parsed.program.body, options);
  const lowerer = new Lowerer(actions, options.source, options.filePath);
  const program: ProgramMessage = {
    functions: [
      {
        name: "main",
        io: {
          inputs: run.params.map((parameter) => {
            if (parameter.type !== "Identifier") {
              throw new Error("validated run parameter changed type");
            }
            return parameter.name;
          }),
          outputs: [],
          span: nodeSpan(run),
        },
        body: lowerer.block(run.body.body, run.body),
        span: nodeSpan(run),
      },
    ],
  };
  const bytes = Buffer.from(Program.encode(program).finish());
  const actionReferences = new Map<string, CompiledActionReference>();
  for (const action of actions.values()) {
    actionReferences.set(`${action.moduleName}:${action.actionName}`, action);
  }

  return {
    actions: [...actionReferences.values()].sort((left, right) =>
      `${left.moduleName}:${left.actionName}`.localeCompare(
        `${right.moduleName}:${right.actionName}`,
      ),
    ),
    bytes,
    hash: createHash("sha256").update(bytes).digest("hex"),
    inputName,
    moduleId: moduleIdentifier(options.filePath, options.projectRoot),
    program,
    workflowName,
  };
}
