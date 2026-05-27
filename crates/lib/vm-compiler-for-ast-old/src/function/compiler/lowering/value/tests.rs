//! Tests for [`super::ValueCompiler`].

use super::*;

use index_type::IndexType;
use waymark_vm_ast_old::{BinaryOperator, DictEntry, Expr};
use waymark_vm_ast_old_helpers::{
    action_call, binary_expr, function_call, int, len_expr, spanned, string,
};
use waymark_vm_bytecode_core::{FunctionId, StateId};
use waymark_vm_compiler_for_ast_old_test_support::{
    TestActionRef, TestConstValue, TestLowering, TestSpec,
};
use waymark_vm_instructions_coreset::CoreSet;
use waymark_vm_instructions_extcallset::ExtCallSet;
use waymark_vm_instructions_fullset::FullSet as InstructionSet;
use waymark_vm_instructions_pureset::PureSet;
use waymark_vm_runtime_core::RegisterId;

use crate::function::compiler::{
    CompilerContextMut,
    bytecode::emitter::FunctionEmitter,
    env::{FlowState, LocalFrame},
    test_helpers::build_function_table,
};

#[test]
fn function_calls_emit_call_then_await() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_call(
                values
                    .plan_function_call(&function_call("child", vec![int(1)]))
                    .expect("function call should plan"),
                ResultTarget::Allocate,
            )
            .expect("function call should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), RegisterId(0));
    assert_eq!(states.len().to_scalar(), 2);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Call {
            dst,
            function_id,
            args,
        })) if *dst == RegisterId(0)
            && *function_id == FunctionId(0)
            && args == &[RegisterId(1)]
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
            if *dst == RegisterId(0)
                && *src == RegisterId(0)
                && *resume == StateId(1)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn action_calls_emit_extcall_then_await_in_resume_state() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_call(
                values
                    .plan_action_call(&action_call("fetch", vec![("value", int(2))]))
                    .expect("action call should plan"),
                ResultTarget::Allocate,
            )
            .expect("action call should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), RegisterId(0));
    assert_eq!(states.len().to_scalar(), 3);

    let mut start_instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == RegisterId(0)
            && action_ref == "fetch"
            && args == &[RegisterId(1)]
            && *resume == StateId(1)
    ));
    assert!(start_instructions.next().is_none());

    let mut resume_instructions = states[StateId(1)].instructions.iter();
    assert!(matches!(
        resume_instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
            if *dst == RegisterId(0)
                && *src == RegisterId(0)
                && *resume == StateId(2)
    ));
    assert!(resume_instructions.next().is_none());
}

#[test]
fn sleep_statements_emit_sleep_then_await_in_resume_state() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let duration = int(2);

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_sleep_statement(&duration)
            .expect("sleep statement should compile");
    }

    let states = emitter.finish();
    assert_eq!(states.len().to_scalar(), 3);

    let mut start_instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::Sleep {
            dst,
            duration,
            resume,
        })) if *dst == RegisterId(0)
            && *duration == RegisterId(1)
            && *resume == StateId(1)
    ));
    assert!(start_instructions.next().is_none());

    let mut resume_instructions = states[StateId(1)].instructions.iter();
    assert!(matches!(
        resume_instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
            if *dst == RegisterId(0)
                && *src == RegisterId(0)
                && *resume == StateId(2)
    ));
    assert!(resume_instructions.next().is_none());
}

#[test]
fn return_statements_without_values_emit_none_return() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_return_statement(None)
            .expect("return without a value should compile");
    }

    let states = emitter.finish();
    assert_eq!(states.len().to_scalar(), 1);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::None,
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Return { src })) if *src == RegisterId(0)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn literals_use_preferred_dst_without_allocating_more_registers() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let preferred_dst = local_frame.allocate_register();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&int(7), ResultTarget::Existing(preferred_dst))
            .expect("literal with preferred dst should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), preferred_dst);
    assert_eq!(local_frame.num_registers(), 1);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(7),
        })) if *dst == preferred_dst
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn add_expressions_use_preferred_dst_for_the_result_register() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let preferred_dst = local_frame.allocate_register();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(
                &binary_expr(int(1), BinaryOperator::Add, int(2)),
                ResultTarget::Existing(preferred_dst),
            )
            .expect("add expression with preferred dst should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), preferred_dst);
    assert_eq!(local_frame.num_registers(), 3);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(2)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::Binary {
            kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
            op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
        }))
            if *dst == preferred_dst
                && *a == RegisterId(1)
                && *b == RegisterId(2)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn function_calls_use_preferred_dst_for_the_result_register() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let preferred_dst = local_frame.allocate_register();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_call(
                values
                    .plan_function_call(&function_call("child", vec![int(1)]))
                    .expect("function call should plan"),
                ResultTarget::Existing(preferred_dst),
            )
            .expect("function call with preferred dst should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), preferred_dst);
    assert_eq!(local_frame.num_registers(), 2);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Call {
            dst,
            function_id,
            args,
        })) if *dst == preferred_dst
            && *function_id == FunctionId(0)
            && args == &[RegisterId(1)]
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
            if *dst == preferred_dst
                && *src == preferred_dst
                && *resume == StateId(1)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn action_calls_use_preferred_dst_for_the_result_register() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let preferred_dst = local_frame.allocate_register();
    let mut flow_state = FlowState::new();

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_call(
                values
                    .plan_action_call(&action_call("fetch", vec![("value", int(2))]))
                    .expect("action call should plan"),
                ResultTarget::Existing(preferred_dst),
            )
            .expect("action call with preferred dst should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), preferred_dst);
    assert_eq!(local_frame.num_registers(), 2);

    let mut start_instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        start_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == preferred_dst
            && *action_ref == "fetch"
            && args == &[RegisterId(1)]
            && *resume == StateId(1)
    ));
    assert!(start_instructions.next().is_none());

    let mut resume_instructions = states[StateId(1)].instructions.iter();
    assert!(matches!(
        resume_instructions.next(),
        Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
            if *dst == preferred_dst
                && *src == preferred_dst
                && *resume == StateId(2)
    ));
    assert!(resume_instructions.next().is_none());
}

#[test]
fn expression_statements_reuse_temporary_result_registers() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expression_statement(&int(1))
            .expect("first expression statement should compile");
        values
            .compile_expression_statement(&int(2))
            .expect("second expression statement should compile");
    }

    let states = emitter.finish();
    assert_eq!(local_frame.num_registers(), 1);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(0)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn action_statements_reuse_temporary_result_registers() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_action_statement(&action_call("fetch_first", Vec::new()))
            .expect("first action statement should compile");
        values
            .compile_action_statement(&action_call("fetch_second", Vec::new()))
            .expect("second action statement should compile");
    }

    let states = emitter.finish();
    assert_eq!(local_frame.num_registers(), 1);

    let mut first_state_instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        first_state_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == RegisterId(0)
            && *action_ref == "fetch_first"
            && args.is_empty()
            && *resume == StateId(1)
    ));
    assert!(first_state_instructions.next().is_none());

    let mut third_state_instructions = states[StateId(2)].instructions.iter();
    assert!(matches!(
        third_state_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == RegisterId(0)
            && *action_ref == "fetch_second"
            && args.is_empty()
            && *resume == StateId(3)
    ));
    assert!(third_state_instructions.next().is_none());
}

#[test]
fn action_statements_reuse_temporary_argument_registers() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_action_statement(&action_call("fetch_first", vec![("value", int(1))]))
            .expect("first action statement should compile");
        values
            .compile_action_statement(&action_call("fetch_second", vec![("value", int(2))]))
            .expect("second action statement should compile");
    }

    let states = emitter.finish();
    assert_eq!(local_frame.num_registers(), 2);

    let mut first_state_instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        first_state_instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        first_state_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == RegisterId(0)
            && *action_ref == "fetch_first"
            && args == &[RegisterId(1)]
            && *resume == StateId(1)
    ));
    assert!(first_state_instructions.next().is_none());

    let mut third_state_instructions = states[StateId(2)].instructions.iter();
    assert!(matches!(
        third_state_instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        third_state_instructions.next(),
        Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
            dst,
            action_ref: TestActionRef(action_ref),
            args,
            resume,
        })) if *dst == RegisterId(0)
            && *action_ref == "fetch_second"
            && args == &[RegisterId(1)]
            && *resume == StateId(3)
    ));
    assert!(third_state_instructions.next().is_none());
}

#[test]
fn list_expressions_emit_make_list() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let expr = spanned(Expr::List {
        elements: vec![int(1), int(2)],
    });

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&expr, ResultTarget::Allocate)
            .expect("list expression should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), RegisterId(2));
    assert_eq!(local_frame.num_registers(), 3);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
            if *dst == RegisterId(2) && items == &[RegisterId(0), RegisterId(1)]
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn dict_expressions_emit_make_dict() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let expr = spanned(Expr::Dict {
        entries: vec![DictEntry {
            key: int(1),
            value: int(2),
        }],
    });

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&expr, ResultTarget::Allocate)
            .expect("dict expression should compile")
    };

    let states = emitter.finish();
    assert_eq!(dst.register(), RegisterId(2));
    assert_eq!(local_frame.num_registers(), 3);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::MakeDict { dst, entries }))
            if *dst == RegisterId(2)
                && entries.len() == 1
                && entries[0].key == RegisterId(0)
                && entries[0].value == RegisterId(1)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn global_len_function_calls_emit_length_instruction() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let expr = len_expr(spanned(Expr::List {
        elements: vec![int(1), int(2)],
    }));

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&expr, ResultTarget::Allocate)
            .expect("len expression should compile")
    };

    let result_register = dst.register();
    let states = emitter.finish();
    assert_eq!(local_frame.num_registers(), 3);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
            if *dst == RegisterId(2) && items == &[RegisterId(0), RegisterId(1)]
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::Length { dst, src }))
            if *dst == result_register && *src == RegisterId(2)
    ));
    assert!(instructions.next().is_none());
}

#[test]
fn index_expressions_emit_index() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let expr = spanned(Expr::Index {
        object: Box::new(spanned(Expr::List {
            elements: vec![int(3), int(4)],
        })),
        index: Box::new(int(1)),
    });

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&expr, ResultTarget::Allocate)
            .expect("index expression should compile")
    };

    let states = emitter.finish();

    let mut instructions = states[StateId(0)].instructions.iter();
    let first_item = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(3),
        })) => *dst,
        other => panic!("unexpected first instruction: {other:?}"),
    };
    let second_item = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(4),
        })) => *dst,
        other => panic!("unexpected second instruction: {other:?}"),
    };
    let list_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
            if items == &[first_item, second_item] =>
        {
            *dst
        }
        other => panic!("unexpected make-list instruction: {other:?}"),
    };
    let index_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) => *dst,
        other => panic!("unexpected index literal instruction: {other:?}"),
    };
    let result_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::Index { dst, object, index }))
            if *object == list_register && *index == index_register =>
        {
            *dst
        }
        other => panic!("unexpected index instruction: {other:?}"),
    };

    assert_eq!(dst.register(), result_register);
    assert!(instructions.next().is_none());
}

#[test]
fn dot_expressions_emit_dot() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();
    let expr = spanned(Expr::Dot {
        object: Box::new(spanned(Expr::Dict {
            entries: vec![DictEntry {
                key: string("field"),
                value: int(9),
            }],
        })),
        attribute: "field".to_owned(),
    });

    let dst = {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expr(&expr, ResultTarget::Allocate)
            .expect("dot expression should compile")
    };

    let states = emitter.finish();

    let mut instructions = states[StateId(0)].instructions.iter();
    let key_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::String(value),
        })) if value == "field" => *dst,
        other => panic!("unexpected dict-key instruction: {other:?}"),
    };
    let value_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(9),
        })) => *dst,
        other => panic!("unexpected dict-value instruction: {other:?}"),
    };
    let object_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::MakeDict { dst, entries }))
            if entries.len() == 1
                && entries[0].key == key_register
                && entries[0].value == value_register =>
        {
            *dst
        }
        other => panic!("unexpected make-dict instruction: {other:?}"),
    };
    let result_register = match instructions.next() {
        Some(InstructionSet::PureSet(PureSet::Dot {
            dst,
            object,
            attribute,
        })) if *object == object_register && attribute == "field" => *dst,
        other => panic!("unexpected dot instruction: {other:?}"),
    };

    assert_eq!(dst.register(), result_register);
    assert!(instructions.next().is_none());
}

#[test]
fn nested_adds_release_temporary_registers_after_use() {
    let function_table = build_function_table();
    let mut emitter = FunctionEmitter::<TestSpec>::new();
    let mut local_frame = LocalFrame::new();
    let mut flow_state = FlowState::new();

    {
        let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
            CompilerContextMut::new(
                &function_table,
                &mut emitter,
                &mut local_frame,
                &mut flow_state,
            )
            .into_ref(),
        );

        values
            .compile_expression_statement(&binary_expr(
                binary_expr(int(1), BinaryOperator::Add, int(2)),
                BinaryOperator::Add,
                int(3),
            ))
            .expect("nested add expression statement should compile");
    }

    let states = emitter.finish();
    assert_eq!(local_frame.num_registers(), 3);

    let mut instructions = states[StateId(0)].instructions.iter();
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(1),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(2),
        })) if *dst == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::Binary {
            kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
            op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
        }))
            if *dst == RegisterId(2)
                && *a == RegisterId(0)
                && *b == RegisterId(1)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::LoadConst {
            dst,
            value: TestConstValue::Int(3),
        })) if *dst == RegisterId(0)
    ));
    assert!(matches!(
        instructions.next(),
        Some(InstructionSet::PureSet(PureSet::Binary {
            kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
            op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
        }))
            if *dst == RegisterId(1)
                && *a == RegisterId(2)
                && *b == RegisterId(0)
    ));
    assert!(instructions.next().is_none());
}
