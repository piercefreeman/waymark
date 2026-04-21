'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const astProto = require('../src/generated/ast_pb.js');
const { transformSource } = require('../src/compiler/transform.js');

function makeTempProject() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'waymark-nextjs-'));
}

describe('transformSource', () => {
  test('registers local action exports in action modules', () => {
    const projectRoot = makeTempProject();
    const actionsPath = path.join(projectRoot, 'actions.ts');

    fs.writeFileSync(
      actionsPath,
      [
        '// use action',
        'export async function fetchUser(userId) {',
        '  return userId;',
        '}',
        ''
      ].join('\n')
    );

    const result = transformSource(fs.readFileSync(actionsPath, 'utf8'), {
      projectRoot,
      resourcePath: actionsPath
    });

    expect(result.transformed).toBe(true);
    expect(result.code).toContain("import { __waymarkRegisterAction } from \"@waymark/nextjs\";");
    expect(result.code).toContain(
      '__waymarkRegisterAction("actions.ts", "fetchUser", fetchUser, ["userId"]);'
    );
  });

  test('compiles a workflow that imports an action from another module', () => {
    const projectRoot = makeTempProject();
    const actionsPath = path.join(projectRoot, 'actions.ts');
    const workflowPath = path.join(projectRoot, 'workflow.ts');

    fs.writeFileSync(
      actionsPath,
      [
        '// use action',
        'export async function fetchUser(userId) {',
        '  return userId;',
        '}',
        ''
      ].join('\n')
    );
    fs.writeFileSync(
      workflowPath,
      [
        "import { Workflow } from '@waymark/nextjs';",
        "import { fetchUser } from './actions';",
        '',
        'export class DemoWorkflow extends Workflow {',
        "  async run(userId = 'fallback') {",
        '    const user = await fetchUser(userId);',
        '    return user;',
        '  }',
        '}',
        ''
      ].join('\n')
    );

    const result = transformSource(fs.readFileSync(workflowPath, 'utf8'), {
      projectRoot,
      resourcePath: workflowPath
    });

    expect(result.transformed).toBe(true);
    expect(result.code).toContain('__waymarkCompiledWorkflow');
    expect(result.code).toContain('__waymarkRunCompiled');
    expect(result.code).not.toMatch(/import ['"]\.\/actions['"];/);
    expect(result.dependencies).toContain(actionsPath);

    const workflow = result.workflows[0];
    expect(workflow.workflowName).toBe('demoworkflow');
    expect(workflow.inputNames).toEqual(['userId']);

    const program = astProto.Program.deserializeBinary(
      Buffer.from(workflow.programBase64, 'base64')
    );
    const bodyStatements = program.getFunctionsList()[0].getBody().getStatementsList();
    expect(bodyStatements).toHaveLength(2);

    const assignment = bodyStatements[0].getAssignment();
    expect(assignment.getTargetsList()).toEqual(['user']);

    const actionCall = assignment.getValue().getActionCall();
    expect(actionCall.getActionName()).toBe('fetchUser');
    expect(actionCall.getModuleName()).toBe('actions.ts');
    expect(actionCall.getKwargsList()).toHaveLength(1);
    expect(actionCall.getKwargsList()[0].getName()).toBe('userId');

    const returnValue = bodyStatements[1].getReturnStmt().getValue().getVariable();
    expect(returnValue.getName()).toBe('user');
  });

  test('normalizes return await action(...) into assignment plus return', () => {
    const projectRoot = makeTempProject();
    const actionsPath = path.join(projectRoot, 'actions.ts');
    const workflowPath = path.join(projectRoot, 'workflow.ts');

    fs.writeFileSync(
      actionsPath,
      [
        '// use action',
        'export async function fetchUser(userId) {',
        '  return userId;',
        '}',
        ''
      ].join('\n')
    );
    fs.writeFileSync(
      workflowPath,
      [
        "import { Workflow } from '@waymark/nextjs';",
        "import { fetchUser } from './actions';",
        '',
        'export class DemoWorkflow extends Workflow {',
        "  async run(userId = 'fallback') {",
        '    return await fetchUser(userId);',
        '  }',
        '}',
        ''
      ].join('\n')
    );

    const result = transformSource(fs.readFileSync(workflowPath, 'utf8'), {
      projectRoot,
      resourcePath: workflowPath
    });

    const workflow = result.workflows[0];
    const program = astProto.Program.deserializeBinary(
      Buffer.from(workflow.programBase64, 'base64')
    );
    const bodyStatements = program.getFunctionsList()[0].getBody().getStatementsList();
    expect(bodyStatements).toHaveLength(2);

    const assignment = bodyStatements[0].getAssignment();
    expect(assignment.getTargetsList()).toEqual(['__waymark_returnTmp_0']);
    expect(assignment.getValue().getActionCall().getActionName()).toBe('fetchUser');

    const returnValue = bodyStatements[1].getReturnStmt().getValue().getVariable();
    expect(returnValue.getName()).toBe('__waymark_returnTmp_0');
  });

  test('lowers Promise.all(collection.map(...action...)) into a spread expression', () => {
    const projectRoot = makeTempProject();
    const workflowPath = path.join(projectRoot, 'workflow.ts');

    fs.writeFileSync(
      workflowPath,
      [
        "import { Workflow } from '@waymark/nextjs';",
        '',
        '// use action',
        'export async function sendEmail(user) {',
        '  return user;',
        '}',
        '',
        'export class BatchWorkflow extends Workflow {',
        '  async run(users) {',
        '    return await Promise.all(users.map((user) => sendEmail(user)));',
        '  }',
        '}',
        ''
      ].join('\n')
    );

    const result = transformSource(fs.readFileSync(workflowPath, 'utf8'), {
      projectRoot,
      resourcePath: workflowPath
    });

    const workflow = result.workflows[0];
    const program = astProto.Program.deserializeBinary(
      Buffer.from(workflow.programBase64, 'base64')
    );
    const bodyStatements = program.getFunctionsList()[0].getBody().getStatementsList();
    expect(bodyStatements).toHaveLength(2);

    const assignment = bodyStatements[0].getAssignment();
    expect(assignment.getTargetsList()).toEqual(['__waymark_returnTmp_0']);

    const spreadExpr = assignment.getValue().getSpreadExpr();
    expect(spreadExpr.getLoopVar()).toBe('user');
    expect(spreadExpr.getAction().getActionName()).toBe('sendEmail');
    expect(spreadExpr.getAction().getKwargsList()[0].getName()).toBe('user');

    const returnValue = bodyStatements[1].getReturnStmt().getValue().getVariable();
    expect(returnValue.getName()).toBe('__waymark_returnTmp_0');
  });
});
