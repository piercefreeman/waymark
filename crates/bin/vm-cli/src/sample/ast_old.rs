pub fn program() -> waymark_vm_ast_old::Program {
    use waymark_vm_ast_old_helpers::*;

    let f1 = function(
        "f1",
        &[],
        vec![
            assignment("x", int(2)),
            assignment("y", int(3)),
            return_stmt(Some(add(variable("x"), variable("y")))),
        ],
    );

    let main_fn = function(
        "main",
        &[],
        vec![
            assignment("a", function_expr("f1", vec![])),
            assignment("b", function_expr("f1", vec![])),
            return_stmt(Some(add(variable("a"), variable("b")))),
        ],
    );

    let functions = vec![main_fn, f1];

    waymark_vm_ast_old::Program { functions }
}
