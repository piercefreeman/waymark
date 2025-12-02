//! Convert IR (Intermediate Representation) to human-readable DSL text format.
//!
//! This module provides formatting utilities to convert the protobuf IR
//! into the Rappel DSL syntax defined in scratch/rappel_grammar.md.

use crate::ir_parser::proto as ir;

/// Format a workflow IR to DSL text
pub fn format_workflow(workflow: &ir::Workflow) -> String {
    let mut out = String::new();
    let mut formatter = DslFormatter::new(&mut out);
    formatter.format_workflow(workflow);
    out
}

struct DslFormatter<'a> {
    out: &'a mut String,
    indent: usize,
}

impl<'a> DslFormatter<'a> {
    fn new(out: &'a mut String) -> Self {
        Self { out, indent: 0 }
    }

    fn write(&mut self, s: &str) {
        self.out.push_str(s);
    }

    fn writeln(&mut self, s: &str) {
        self.write_indent();
        self.out.push_str(s);
        self.out.push('\n');
    }

    fn write_indent(&mut self) {
        for _ in 0..self.indent {
            self.out.push_str("    ");
        }
    }

    fn format_workflow(&mut self, workflow: &ir::Workflow) {
        // workflow name(params) -> return_type:
        self.write("workflow ");
        self.write(&workflow.name);
        self.write("(");

        for (i, param) in workflow.params.iter().enumerate() {
            if i > 0 {
                self.write(", ");
            }
            self.write(&param.name);
            if let Some(type_ann) = &param.type_annotation {
                self.write(": ");
                self.write(type_ann);
            }
        }
        self.write(")");

        if let Some(ret_type) = &workflow.return_type {
            self.write(" -> ");
            self.write(ret_type);
        }
        self.write(":\n");

        self.indent += 1;
        for stmt in &workflow.body {
            self.format_statement(stmt);
        }
        self.indent -= 1;
    }

    fn format_statement(&mut self, stmt: &ir::Statement) {
        let kind = match &stmt.kind {
            Some(k) => k,
            None => return,
        };

        match kind {
            ir::statement::Kind::ActionCall(action) => {
                self.format_action_call(action);
            }
            ir::statement::Kind::Gather(gather) => {
                self.format_gather(gather);
            }
            ir::statement::Kind::PythonBlock(block) => {
                self.format_python_block(block);
            }
            ir::statement::Kind::Loop(loop_) => {
                self.format_loop(loop_);
            }
            ir::statement::Kind::Conditional(cond) => {
                self.format_conditional(cond);
            }
            ir::statement::Kind::TryExcept(te) => {
                self.format_try_except(te);
            }
            ir::statement::Kind::Sleep(sleep) => {
                self.format_sleep(sleep);
            }
            ir::statement::Kind::ReturnStmt(ret) => {
                self.format_return(ret);
            }
            ir::statement::Kind::Spread(spread) => {
                self.format_spread(spread);
            }
        }
    }

    fn format_action_call(&mut self, action: &ir::ActionCall) {
        self.write_indent();

        // target = @action(kwargs) [policy]
        if let Some(target) = &action.target {
            self.write(target);
            self.write(" = ");
        }

        self.write("@");
        if let Some(module) = &action.module {
            self.write(module);
            self.write(".");
        }
        self.write(&action.action);
        self.write("(");

        let mut first = true;
        for (key, value) in &action.kwargs {
            if !first {
                self.write(", ");
            }
            first = false;
            self.write(key);
            self.write("=");
            self.write(value);
        }
        self.write(")");

        // Format policy if present
        if let Some(config) = &action.config {
            self.format_policy(config);
        }

        self.write("\n");
    }

    fn format_policy(&mut self, config: &ir::RunActionConfig) {
        let mut parts = Vec::new();

        if let Some(timeout) = config.timeout_seconds {
            parts.push(format!("timeout={}s", timeout));
        }
        if let Some(retries) = config.max_retries {
            parts.push(format!("retry={}", retries));
        }
        if let Some(backoff) = &config.backoff {
            let backoff_str = match ir::backoff_config::Kind::try_from(backoff.kind) {
                Ok(ir::backoff_config::Kind::Linear) | Ok(ir::backoff_config::Kind::Unspecified) => {
                    format!("backoff=linear({}ms)", backoff.base_delay_ms)
                }
                Ok(ir::backoff_config::Kind::Exponential) => {
                    let mult = backoff.multiplier.unwrap_or(2.0);
                    format!("backoff=exp({}ms, {}x)", backoff.base_delay_ms, mult)
                }
                Err(_) => String::new(),
            };
            if !backoff_str.is_empty() {
                parts.push(backoff_str);
            }
        }

        if !parts.is_empty() {
            self.write(" [policy: ");
            self.write(&parts.join(", "));
            self.write("]");
        }
    }

    fn format_gather(&mut self, gather: &ir::Gather) {
        self.write_indent();

        if let Some(target) = &gather.target {
            self.write(target);
            self.write(" = ");
        }

        self.write("parallel(\n");
        self.indent += 1;

        for call in &gather.calls {
            if let Some(kind) = &call.kind {
                match kind {
                    ir::gather_call::Kind::Action(action) => {
                        self.format_action_call(action);
                    }
                    ir::gather_call::Kind::Subgraph(subgraph) => {
                        self.format_subgraph_call(subgraph);
                    }
                }
            }
        }

        self.indent -= 1;
        self.writeln(")");
    }

    fn format_subgraph_call(&mut self, subgraph: &ir::SubgraphCall) {
        self.write_indent();

        if let Some(target) = &subgraph.target {
            self.write(target);
            self.write(" = ");
        }

        self.write("self.");
        self.write(&subgraph.method_name);
        self.write("(");

        let mut first = true;
        for (key, value) in &subgraph.kwargs {
            if !first {
                self.write(", ");
            }
            first = false;
            self.write(key);
            self.write("=");
            self.write(value);
        }
        self.write(")\n");
    }

    fn format_python_block(&mut self, block: &ir::PythonBlock) {
        self.write_indent();
        self.write("python");

        // IO spec
        let has_io = !block.inputs.is_empty() || !block.outputs.is_empty();
        if has_io {
            self.write("(");
            let mut parts = Vec::new();
            if !block.inputs.is_empty() {
                parts.push(format!("reads: {}", block.inputs.join(", ")));
            }
            if !block.outputs.is_empty() {
                parts.push(format!("writes: {}", block.outputs.join(", ")));
            }
            self.write(&parts.join("; "));
            self.write(")");
        }

        self.write(" {\n");

        // Format code with proper indentation
        self.indent += 1;
        for line in block.code.lines() {
            self.write_indent();
            self.write(line);
            self.write("\n");
        }
        self.indent -= 1;

        self.writeln("}");
    }

    fn format_loop(&mut self, loop_: &ir::Loop) {
        self.write_indent();
        self.write("loop ");
        self.write(&loop_.loop_var);
        self.write(" in ");
        self.write(&loop_.iterator_expr);
        self.write(" -> [");
        self.write(&loop_.accumulator);
        self.write("]:\n");

        self.indent += 1;
        for stmt in &loop_.body {
            self.format_statement(stmt);
        }
        self.indent -= 1;
    }

    fn format_conditional(&mut self, cond: &ir::Conditional) {
        for (i, branch) in cond.branches.iter().enumerate() {
            self.write_indent();

            if i == 0 {
                self.write("branch if ");
            } else if branch.guard.is_empty() {
                self.write("branch else");
            } else {
                self.write("branch elif ");
            }

            if !branch.guard.is_empty() {
                self.write(&branch.guard);
            }
            self.write(":\n");

            self.indent += 1;

            // Preamble
            for pre in &branch.preamble {
                self.format_python_block(pre);
            }

            // Actions
            for action in &branch.actions {
                self.format_action_call(action);
            }

            // Postamble
            for post in &branch.postamble {
                self.format_python_block(post);
            }

            self.indent -= 1;
        }
    }

    fn format_try_except(&mut self, te: &ir::TryExcept) {
        self.writeln("try:");
        self.indent += 1;

        // Try preamble
        for pre in &te.try_preamble {
            self.format_python_block(pre);
        }

        // Try body
        for action in &te.try_body {
            self.format_action_call(action);
        }

        // Try postamble
        for post in &te.try_postamble {
            self.format_python_block(post);
        }

        self.indent -= 1;

        // Handlers
        for handler in &te.handlers {
            self.write_indent();
            self.write("except");

            if !handler.exception_types.is_empty() {
                self.write(" ");
                let types: Vec<String> = handler.exception_types.iter()
                    .map(|et| {
                        let mut s = String::new();
                        if let Some(module) = &et.module {
                            s.push_str(module);
                            s.push('.');
                        }
                        if let Some(name) = &et.name {
                            s.push_str(name);
                        }
                        s
                    })
                    .filter(|s| !s.is_empty())
                    .collect();

                if types.len() == 1 {
                    self.write(&types[0]);
                } else if types.len() > 1 {
                    self.write("(");
                    self.write(&types.join(", "));
                    self.write(")");
                }
            }
            self.write(":\n");

            self.indent += 1;

            // Handler preamble
            for pre in &handler.preamble {
                self.format_python_block(pre);
            }

            // Handler body
            for action in &handler.body {
                self.format_action_call(action);
            }

            // Handler postamble
            for post in &handler.postamble {
                self.format_python_block(post);
            }

            self.indent -= 1;
        }
    }

    fn format_sleep(&mut self, sleep: &ir::Sleep) {
        self.write_indent();
        self.write("@sleep(");
        self.write(&sleep.duration_expr);
        self.write(")\n");
    }

    fn format_return(&mut self, ret: &ir::Return) {
        self.write_indent();
        self.write("return");

        if let Some(value) = &ret.value {
            self.write(" ");
            match value {
                ir::r#return::Value::Expr(expr) => {
                    self.write(expr);
                }
                ir::r#return::Value::Action(action) => {
                    // Inline action without newline
                    self.write("@");
                    if let Some(module) = &action.module {
                        self.write(module);
                        self.write(".");
                    }
                    self.write(&action.action);
                    self.write("(");
                    let mut first = true;
                    for (key, value) in &action.kwargs {
                        if !first {
                            self.write(", ");
                        }
                        first = false;
                        self.write(key);
                        self.write("=");
                        self.write(value);
                    }
                    self.write(")");
                }
                ir::r#return::Value::Gather(_gather) => {
                    self.write("parallel(...)");  // Simplified
                }
            }
        }
        self.write("\n");
    }

    fn format_spread(&mut self, spread: &ir::Spread) {
        self.write_indent();

        if let Some(target) = &spread.target {
            self.write(target);
            self.write(" = ");
        }

        self.write("spread @");
        if let Some(action) = &spread.action {
            if let Some(module) = &action.module {
                self.write(module);
                self.write(".");
            }
            self.write(&action.action);
            self.write("(");
            let mut first = true;
            for (key, value) in &action.kwargs {
                if !first {
                    self.write(", ");
                }
                first = false;
                self.write(key);
                self.write("=");
                self.write(value);
            }
            self.write(")");
        }

        self.write(" over ");
        self.write(&spread.iterable);
        self.write(" as ");
        self.write(&spread.loop_var);
        self.write("\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_action(name: &str, target: Option<&str>) -> ir::ActionCall {
        ir::ActionCall {
            action: name.to_string(),
            module: Some("test_module".to_string()),
            kwargs: HashMap::new(),
            target: target.map(|s| s.to_string()),
            config: None,
            location: None,
        }
    }

    #[test]
    fn test_simple_workflow() {
        let workflow = ir::Workflow {
            name: "my_workflow".to_string(),
            params: vec![
                ir::WorkflowParam {
                    name: "x".to_string(),
                    type_annotation: Some("int".to_string()),
                },
            ],
            body: vec![
                ir::Statement {
                    kind: Some(ir::statement::Kind::ActionCall(make_action("fetch", Some("data")))),
                },
                ir::Statement {
                    kind: Some(ir::statement::Kind::ReturnStmt(ir::Return {
                        value: Some(ir::r#return::Value::Expr("data".to_string())),
                        location: None,
                    })),
                },
            ],
            return_type: Some("str".to_string()),
        };

        let output = format_workflow(&workflow);
        assert!(output.contains("workflow my_workflow(x: int) -> str:"));
        assert!(output.contains("data = @test_module.fetch()"));
        assert!(output.contains("return data"));
    }

    #[test]
    fn test_gather() {
        let workflow = ir::Workflow {
            name: "parallel_workflow".to_string(),
            params: vec![],
            body: vec![
                ir::Statement {
                    kind: Some(ir::statement::Kind::Gather(ir::Gather {
                        calls: vec![
                            ir::GatherCall {
                                kind: Some(ir::gather_call::Kind::Action(make_action("fetch_a", None))),
                            },
                            ir::GatherCall {
                                kind: Some(ir::gather_call::Kind::Action(make_action("fetch_b", None))),
                            },
                        ],
                        target: Some("results".to_string()),
                        location: None,
                    })),
                },
            ],
            return_type: None,
        };

        let output = format_workflow(&workflow);
        assert!(output.contains("results = parallel("));
        assert!(output.contains("@test_module.fetch_a()"));
        assert!(output.contains("@test_module.fetch_b()"));
    }

    #[test]
    fn test_loop() {
        let workflow = ir::Workflow {
            name: "loop_workflow".to_string(),
            params: vec![],
            body: vec![
                ir::Statement {
                    kind: Some(ir::statement::Kind::Loop(ir::Loop {
                        iterator_expr: "items".to_string(),
                        loop_var: "item".to_string(),
                        accumulator: "results".to_string(),
                        body: vec![
                            ir::Statement {
                                kind: Some(ir::statement::Kind::ActionCall(make_action("process", Some("result")))),
                            },
                        ],
                        location: None,
                    })),
                },
            ],
            return_type: None,
        };

        let output = format_workflow(&workflow);
        assert!(output.contains("loop item in items -> [results]:"));
        assert!(output.contains("result = @test_module.process()"));
    }
}
