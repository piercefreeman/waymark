pub fn program() -> waymark_vm_ast_old::Program {
    use waymark_vm_ast_old_helpers::*;

    // f1():
    //   x = 2
    //   y = 3
    //   return x + y
    let f1 = function(
        "f1",
        &[],
        vec![
            assignment("x", int(2)),
            assignment("y", int(3)),
            return_stmt(Some(binary_expr(
                variable("x"),
                waymark_vm_ast_old::BinaryOperator::Add,
                variable("y"),
            ))),
        ],
    );

    // main():
    //   a = f1()
    //   b = f1()
    //   return a + b
    let main_fn = function(
        "main",
        &[],
        vec![
            assignment("a", function_expr("f1", vec![])),
            assignment("b", function_expr("f1", vec![])),
            return_stmt(Some(binary_expr(
                variable("a"),
                waymark_vm_ast_old::BinaryOperator::Add,
                variable("b"),
            ))),
        ],
    );

    let functions = vec![main_fn, f1];

    waymark_vm_ast_old::Program { functions }
}
