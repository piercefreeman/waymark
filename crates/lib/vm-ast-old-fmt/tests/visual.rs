use waymark_vm_ast_old::{
    BinaryOperator, Call, DictEntry, DurationLiteral, ElifBranch, ElseBranch, ExceptHandler, Expr,
    FunctionDef, GlobalFunction, IfBranch, IoDecl, PolicyBracket, RetryPolicy, Statement,
    TimeoutPolicy,
};
use waymark_vm_ast_old_helpers::{
    block, builtin_function_call, int, kwarg, module_action_call, program, return_stmt, spanned,
    string, variable,
};

#[test]
fn fmt_display_prints_program() {
    let helper = spanned(FunctionDef {
        name: "helper".to_owned(),
        io: spanned(IoDecl {
            inputs: Vec::new(),
            outputs: vec!["answer".to_owned()],
        }),
        body: block(Vec::new()),
    });

    let main = spanned(FunctionDef {
        name: "main".to_owned(),
        io: spanned(IoDecl {
            inputs: vec!["items".to_owned(), "flag".to_owned()],
            outputs: vec!["result".to_owned()],
        }),
        body: block(vec![
            spanned(Statement::Assignment {
                targets: vec!["results".to_owned()],
                value: spanned(Expr::List {
                    elements: Vec::new(),
                }),
            }),
            spanned(Statement::Assignment {
                targets: vec!["stats".to_owned()],
                value: spanned(Expr::Dict {
                    entries: vec![DictEntry {
                        key: string("count"),
                        value: spanned(Expr::FunctionCall {
                            call: builtin_function_call(
                                GlobalFunction::Len,
                                vec![variable("items")],
                            ),
                        }),
                    }],
                }),
            }),
            spanned(Statement::Assignment {
                targets: vec!["grouped".to_owned()],
                value: spanned(Expr::ParallelExpr {
                    calls: vec![
                        Call::Action(module_action_call(
                            "worker",
                            "fetch",
                            vec![kwarg(
                                "item",
                                spanned(Expr::Index {
                                    object: Box::new(variable("items")),
                                    index: Box::new(int(0)),
                                }),
                            )],
                            vec![PolicyBracket::Timeout(TimeoutPolicy {
                                timeout: DurationLiteral { seconds: 30 },
                            })],
                        )),
                        Call::Function(builtin_function_call(
                            GlobalFunction::Len,
                            vec![variable("items")],
                        )),
                    ],
                }),
            }),
            spanned(Statement::SpreadAction {
                collection: variable("items"),
                loop_var: "item".to_owned(),
                action: module_action_call(
                    "worker",
                    "process",
                    vec![kwarg("item", variable("item"))],
                    vec![PolicyBracket::Retry(RetryPolicy {
                        exception_types: vec!["ValueError".to_owned()],
                        max_retries: 3,
                        backoff: Some(DurationLiteral { seconds: 60 }),
                    })],
                ),
            }),
            spanned(Statement::Conditional {
                if_branch: spanned(IfBranch {
                    condition: spanned(Expr::BinaryOp {
                        left: Box::new(spanned(Expr::UnaryOp {
                            op: waymark_vm_ast_old::UnaryOperator::Not,
                            operand: Box::new(spanned(Expr::Dot {
                                object: Box::new(variable("payload")),
                                attribute: "ready".to_owned(),
                            })),
                        })),
                        op: BinaryOperator::Or,
                        right: Box::new(spanned(Expr::BinaryOp {
                            left: Box::new(spanned(Expr::FunctionCall {
                                call: builtin_function_call(
                                    GlobalFunction::Len,
                                    vec![variable("items")],
                                ),
                            })),
                            op: BinaryOperator::Eq,
                            right: Box::new(int(0)),
                        })),
                    }),
                    body: block(vec![spanned(Statement::Sleep {
                        duration: spanned(Expr::Literal {
                            value: waymark_vm_ast_old::Literal::Int(300),
                        }),
                    })]),
                }),
                elif_branches: vec![spanned(ElifBranch {
                    condition: variable("flag"),
                    body: block(vec![spanned(Statement::ExprStmt {
                        expr: spanned(Expr::ActionCall {
                            call: module_action_call("worker", "audit", Vec::new(), Vec::new()),
                        }),
                    })]),
                })],
                else_branch: Some(spanned(ElseBranch {
                    body: block(vec![spanned(Statement::ActionCall {
                        call: module_action_call("worker", "notify", Vec::new(), Vec::new()),
                    })]),
                })),
            }),
            spanned(Statement::ForLoop {
                loop_vars: vec!["index".to_owned(), "item".to_owned()],
                iterable: spanned(Expr::FunctionCall {
                    call: builtin_function_call(GlobalFunction::Enumerate, vec![variable("items")]),
                }),
                body: block(vec![spanned(Statement::Assignment {
                    targets: vec!["results".to_owned()],
                    value: spanned(Expr::BinaryOp {
                        left: Box::new(variable("results")),
                        op: BinaryOperator::Add,
                        right: Box::new(spanned(Expr::List {
                            elements: vec![variable("item")],
                        })),
                    }),
                })]),
            }),
            spanned(Statement::WhileLoop {
                condition: spanned(Expr::BinaryOp {
                    left: Box::new(spanned(Expr::Dot {
                        object: Box::new(variable("payload")),
                        attribute: "ready".to_owned(),
                    })),
                    op: BinaryOperator::And,
                    right: Box::new(spanned(Expr::BinaryOp {
                        left: Box::new(spanned(Expr::FunctionCall {
                            call: builtin_function_call(
                                GlobalFunction::Len,
                                vec![variable("results")],
                            ),
                        })),
                        op: BinaryOperator::Lt,
                        right: Box::new(spanned(Expr::FunctionCall {
                            call: builtin_function_call(
                                GlobalFunction::Len,
                                vec![variable("items")],
                            ),
                        })),
                    })),
                }),
                body: block(vec![spanned(Statement::Continue)]),
            }),
            spanned(Statement::TryExcept {
                handlers: vec![spanned(ExceptHandler {
                    exception_types: vec!["ValueError".to_owned()],
                    exception_var: Some("err".to_owned()),
                    body: block(vec![return_stmt(Some(variable("err")))]),
                })],
                try_block: block(vec![spanned(Statement::Assignment {
                    targets: vec!["result".to_owned()],
                    value: spanned(Expr::ActionCall {
                        call: module_action_call(
                            "worker",
                            "finish",
                            vec![kwarg("value", variable("grouped"))],
                            vec![PolicyBracket::Timeout(TimeoutPolicy {
                                timeout: DurationLiteral { seconds: 30 },
                            })],
                        ),
                    }),
                })]),
                finally_block: None,
            }),
            return_stmt(Some(spanned(Expr::Dict {
                entries: vec![
                    DictEntry {
                        key: string("results"),
                        value: variable("results"),
                    },
                    DictEntry {
                        key: string("stats"),
                        value: variable("stats"),
                    },
                ],
            }))),
        ]),
    });

    let program = program(vec![helper, main]);

    insta::assert_snapshot!(waymark_vm_ast_old_fmt::display(&program));
}
