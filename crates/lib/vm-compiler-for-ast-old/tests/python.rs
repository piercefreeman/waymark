#[allow(dead_code)]
mod support;

use futures_util::StreamExt as _;
use std::path::{Path, PathBuf};
use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

const SKIP_TESTS: &[&str] = &[
    "fixtures/test_actions.py",
    "fixtures_actions/soak_actions.py",
    "fixtures_gather/gather_run_action_static.py",
    "fixtures_gather/gather_unsupported_variable.py",
    "fixtures_models/pydantic_ast_variants.py",
    "fixtures_unsupported/builtin_call.py",
    "fixtures_unsupported/constructor_assignment.py",
    "fixtures_unsupported/constructor_return.py",
    "fixtures_unsupported/fstring_usage.py",
    "fixtures_unsupported/lambda_expression.py",
    "fixtures_unsupported/list_comprehension.py",
    "fixtures_unsupported/match_workflow.py",
    "fixtures_unsupported/non_action_await.py",
    "fixtures_unsupported/with_statement.py",
    "fixtures_workflow/workflow_default_args.py",
    "fixtures_workflow/workflow_helper_inheritance_base.py",
    "fixtures_workflow/workflow_helper_inheritance_child.py",
];

async fn python_tests<P: Into<PathBuf>>(
    skip_list: impl IntoIterator<Item = P>,
) -> Result<(PathBuf, Vec<PathBuf>), std::io::Error> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("../../..");
    let python_tests_root = repo_root.join("python/tests");

    let mut walk_dir = async_walkdir::WalkDir::new(&python_tests_root);

    let skip_list: Vec<_> = skip_list.into_iter().map(|path| path.into()).collect();

    let mut test_files = Vec::new();
    loop {
        let Some(dir_entry_result) = walk_dir.next().await else {
            break;
        };

        let dir_entry = dir_entry_result?;

        let test_file_path = dir_entry.path();

        let is_py_file = matches!(test_file_path.extension(), Some(ext) if ext == "py");
        if !is_py_file {
            continue;
        }

        let test_file_path = test_file_path
            .strip_prefix(&python_tests_root)
            .map_err(std::io::Error::other)?;

        let components_count = test_file_path.components().count();
        if components_count < 2 {
            continue;
        }

        let is_init = matches!(test_file_path.file_stem(), Some(stem) if stem == "__init__");
        if is_init {
            continue;
        }

        if skip_list.iter().any(|skip| test_file_path == skip) {
            continue;
        }

        test_files.push(test_file_path.to_owned());
    }

    Ok((python_tests_root, test_files))
}

#[tokio::test]
async fn compile_python_tests() {
    let (python_tests_root, mut python_test_files) = python_tests(SKIP_TESTS).await.unwrap();
    python_test_files.sort();

    insta::assert_debug_snapshot!("python_test_files", python_test_files);

    for python_test_file in python_test_files {
        let full_path = python_tests_root.join(&python_test_file);

        let program = waymark_support_test_python::load_fixture_ast_old(&full_path)
            .await
            .unwrap_or_else(|err| panic!("Unable to load {}: {err}", full_path.display()));

        let snapshot = format!("{}/ast", python_test_file.display());
        let details = format!("ast for python/tests/{}", python_test_file.display());
        insta::assert_debug_snapshot!(snapshot, program, &details);

        let bytecode_result =
            waymark_vm_compiler_for_ast_old::compile::<TestSpec, TestLowering>(&program);

        let snapshot = format!("{}/bytecode", python_test_file.display());
        let details = format!("bytecode for python/tests/{}", python_test_file.display());
        insta::assert_debug_snapshot!(snapshot, bytecode_result, &details);
    }
}
