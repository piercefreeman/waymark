"""FastAPI surface for the waymark example app."""

import inspect
import json
import os
import time
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Literal, Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from waymark import (
    bridge,
    delete_schedule,
    pause_schedule,
    resume_schedule,
    schedule_workflow,
)

from example_app.workflows import (
    BranchRequest,
    BranchResult,
    ChainRequest,
    ChainResult,
    ComputationRequest,
    ComputationResult,
    ConditionalBranchWorkflow,
    DurableSleepWorkflow,
    GuardFallbackRequest,
    GuardFallbackResult,
    GuardFallbackWorkflow,
    EarlyReturnLoopResult,
    EarlyReturnLoopWorkflow,
    ErrorHandlingWorkflow,
    ErrorRequest,
    ErrorResult,
    ExceptionMetadataWorkflow,
    KwOnlyLocationRequest,
    KwOnlyLocationResult,
    KwOnlyLocationWorkflow,
    LoopExceptionRequest,
    LoopExceptionResult,
    LoopExceptionWorkflow,
    LoopReturnRequest,
    LoopReturnResult,
    LoopReturnWorkflow,
    LoopProcessingWorkflow,
    LoopRequest,
    LoopResult,
    LoopingSleepRequest,
    LoopingSleepResult,
    LoopingSleepWorkflow,
    RetryCounterRequest,
    RetryCounterResult,
    RetryCounterWorkflow,
    TimeoutProbeRequest,
    TimeoutProbeResult,
    TimeoutProbeWorkflow,
    ManyActionsRequest,
    ManyActionsResult,
    ManyActionsWorkflow,
    NoOpWorkflow,
    ParallelMathWorkflow,
    SequentialChainWorkflow,
    SleepRequest,
    SleepResult,
    SpreadEmptyCollectionWorkflow,
    SpreadEmptyRequest,
    SpreadEmptyResult,
    UndefinedVariableWorkflow,
    WhileLoopRequest,
    WhileLoopResult,
    WhileLoopWorkflow,
)

app = FastAPI(title="Waymark Example")

templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent / "templates")
)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {"request": request},
    )


# =============================================================================
# Parallel Execution (asyncio.gather)
# =============================================================================


@app.post("/api/parallel", response_model=ComputationResult)
async def run_parallel_workflow(payload: ComputationRequest) -> ComputationResult:
    """Run the parallel math workflow demonstrating asyncio.gather."""
    workflow = ParallelMathWorkflow()
    return await workflow.run(number=payload.number)


# =============================================================================
# Sequential Chain
# =============================================================================


@app.post("/api/chain", response_model=ChainResult)
async def run_chain_workflow(payload: ChainRequest) -> ChainResult:
    """Run the sequential chain workflow demonstrating action chaining."""
    workflow = SequentialChainWorkflow()
    return await workflow.run(text=payload.text)


# =============================================================================
# Conditional Branching (if/else)
# =============================================================================


@app.post("/api/branch", response_model=BranchResult)
async def run_branch_workflow(payload: BranchRequest) -> BranchResult:
    """Run the conditional branch workflow demonstrating if/else logic."""
    workflow = ConditionalBranchWorkflow()
    return await workflow.run(value=payload.value)


# =============================================================================
# Loop Processing
# =============================================================================


@app.post("/api/loop", response_model=LoopResult)
async def run_loop_workflow(payload: LoopRequest) -> LoopResult:
    """Run the loop workflow demonstrating iteration."""
    workflow = LoopProcessingWorkflow()
    return await workflow.run(items=payload.items)


# =============================================================================
# While Loop Processing
# =============================================================================


@app.post("/api/while-loop", response_model=WhileLoopResult)
async def run_while_loop_workflow(payload: WhileLoopRequest) -> WhileLoopResult:
    """Run the while-loop workflow demonstrating counter-based iteration."""
    workflow = WhileLoopWorkflow()
    return await workflow.run(limit=payload.limit)


# =============================================================================
# Return Inside Loop
# =============================================================================


@app.post("/api/loop-return", response_model=LoopReturnResult)
async def run_loop_return_workflow(payload: LoopReturnRequest) -> LoopReturnResult:
    """Run the early-return workflow demonstrating return inside a for-loop."""
    workflow = LoopReturnWorkflow()
    return await workflow.run(items=payload.items, needle=payload.needle)


# =============================================================================
# Loop with Exception Handling
# =============================================================================


@app.post("/api/loop-exception", response_model=LoopExceptionResult)
async def run_loop_exception_workflow(
    payload: LoopExceptionRequest,
) -> LoopExceptionResult:
    """
    Run the loop exception workflow demonstrating exception handling inside a for loop.

    Items starting with 'bad' will fail. The workflow catches the exception,
    increments an error counter, and continues to the next item.
    """
    workflow = LoopExceptionWorkflow()
    return await workflow.run(items=payload.items)


# =============================================================================
# Error Handling (try/except)
# =============================================================================


@app.post("/api/error", response_model=ErrorResult)
async def run_error_workflow(payload: ErrorRequest) -> ErrorResult:
    """Run the error handling workflow demonstrating try/except."""
    workflow = ErrorHandlingWorkflow()
    return await workflow.run(should_fail=payload.should_fail)


@app.post("/api/exception-metadata", response_model=ErrorResult)
async def run_exception_metadata_workflow(payload: ErrorRequest) -> ErrorResult:
    """Run the exception metadata workflow demonstrating captured values."""
    workflow = ExceptionMetadataWorkflow()
    return await workflow.run(should_fail=payload.should_fail)


# =============================================================================
# Retry Behavior (file-backed counter)
# =============================================================================


@app.post("/api/retry-counter", response_model=RetryCounterResult)
async def run_retry_counter_workflow(
    payload: RetryCounterRequest,
) -> RetryCounterResult:
    """Run retry workflow with a configurable success threshold and retry count."""
    workflow = RetryCounterWorkflow()
    return await workflow.run(
        succeed_on_attempt=payload.succeed_on_attempt,
        max_attempts=payload.max_attempts,
        counter_slot=payload.counter_slot,
    )


# =============================================================================
# Timeout Behavior
# =============================================================================


@app.post("/api/timeout-probe", response_model=TimeoutProbeResult)
async def run_timeout_probe_workflow(
    payload: TimeoutProbeRequest,
) -> TimeoutProbeResult:
    """Run timeout workflow that always times out for the configured attempts."""
    workflow = TimeoutProbeWorkflow()
    return await workflow.run(
        max_attempts=payload.max_attempts,
        counter_slot=payload.counter_slot,
    )


# =============================================================================
# Durable Sleep
# =============================================================================


@app.post("/api/sleep", response_model=SleepResult)
async def run_sleep_workflow(payload: SleepRequest) -> SleepResult:
    """Run the durable sleep workflow demonstrating asyncio.sleep."""
    workflow = DurableSleepWorkflow()
    return await workflow.run(seconds=payload.seconds)


# =============================================================================
# Guard Fallback (if without else)
# =============================================================================


@app.post("/api/guard-fallback", response_model=GuardFallbackResult)
async def run_guard_fallback_workflow(
    payload: GuardFallbackRequest,
) -> GuardFallbackResult:
    """Run the guard fallback workflow demonstrating default continuation."""
    workflow = GuardFallbackWorkflow()
    return await workflow.run(user=payload.user)


# =============================================================================
# Kw-only Inputs
# =============================================================================


@app.post("/api/kw-only", response_model=KwOnlyLocationResult)
async def run_kw_only_workflow(
    payload: KwOnlyLocationRequest,
) -> KwOnlyLocationResult:
    """Run the kw-only inputs workflow demonstrating request models."""
    workflow = KwOnlyLocationWorkflow()
    return await workflow.run(
        latitude=payload.latitude,
        longitude=payload.longitude,
    )


# =============================================================================
# Undefined Variable Validation
# =============================================================================


class UndefinedVariableRequest(BaseModel):
    input_text: str = Field(description="Sample input (not used by the workflow).")


@app.post("/api/undefined-variable")
async def run_undefined_variable_workflow(payload: UndefinedVariableRequest) -> dict:
    """Trigger a workflow that fails validation due to out-of-scope variables."""
    workflow = UndefinedVariableWorkflow()
    try:
        result = await workflow.run(input_text=payload.input_text)
    except Exception as exc:  # pragma: no cover - example endpoint
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"result": result}


# =============================================================================
# Early Return with Loop (if-return followed by for-loop)
# =============================================================================


class EarlyReturnLoopRequest(BaseModel):
    input_text: str = Field(
        description="Input text to parse. Use 'no_session:' prefix for early return path, or comma-separated items for loop path."
    )


@app.post("/api/early-return-loop", response_model=EarlyReturnLoopResult)
async def run_early_return_loop_workflow(
    payload: EarlyReturnLoopRequest,
) -> EarlyReturnLoopResult:
    """Run the early return + loop workflow demonstrating if-return followed by for-loop."""
    workflow = EarlyReturnLoopWorkflow()
    return await workflow.run(input_text=payload.input_text)


# =============================================================================
# Spread Over Empty Collection (tests empty spread handling in for-loops)
# =============================================================================


@app.post("/api/spread-empty", response_model=SpreadEmptyResult)
async def run_spread_empty_workflow(
    payload: SpreadEmptyRequest,
) -> SpreadEmptyResult:
    """
    Run the spread empty collection workflow demonstrating empty spread handling.

    Test cases:
    - items=["a", "b", "c"] - Normal spread with items
    - items=[] - Empty collection, should handle gracefully
    """
    workflow = SpreadEmptyCollectionWorkflow()
    return await workflow.run(items=payload.items)


# =============================================================================
# Many Actions (stress test)
# =============================================================================


@app.post("/api/many-actions", response_model=ManyActionsResult)
async def run_many_actions_workflow(payload: ManyActionsRequest) -> ManyActionsResult:
    """
    Run the many actions workflow for stress testing.

    Executes a configurable number of actions either in parallel or sequentially.
    """
    workflow = ManyActionsWorkflow()
    return await workflow.run(
        action_count=payload.action_count, parallel=payload.parallel
    )


# =============================================================================
# Looping Sleep (for testing durable sleep in loops)
# =============================================================================


@app.post("/api/looping-sleep", response_model=LoopingSleepResult)
async def run_looping_sleep_workflow(
    payload: LoopingSleepRequest,
) -> LoopingSleepResult:
    """
    Run the looping sleep workflow demonstrating durable sleep in loops.

    Each iteration sleeps for the specified duration, then performs an action.
    Useful for testing looping sleep workflows.
    """
    workflow = LoopingSleepWorkflow()
    return await workflow.run(
        iterations=payload.iterations, sleep_seconds=payload.sleep_seconds
    )


# =============================================================================
# Scheduled Workflows
# =============================================================================

# Mapping of workflow names to classes for dynamic lookup
WORKFLOW_REGISTRY = {
    "ParallelMathWorkflow": ParallelMathWorkflow,
    "SequentialChainWorkflow": SequentialChainWorkflow,
    "ConditionalBranchWorkflow": ConditionalBranchWorkflow,
    "LoopProcessingWorkflow": LoopProcessingWorkflow,
    "WhileLoopWorkflow": WhileLoopWorkflow,
    "LoopExceptionWorkflow": LoopExceptionWorkflow,
    "ErrorHandlingWorkflow": ErrorHandlingWorkflow,
    "ExceptionMetadataWorkflow": ExceptionMetadataWorkflow,
    "RetryCounterWorkflow": RetryCounterWorkflow,
    "TimeoutProbeWorkflow": TimeoutProbeWorkflow,
    "DurableSleepWorkflow": DurableSleepWorkflow,
    "GuardFallbackWorkflow": GuardFallbackWorkflow,
    "EarlyReturnLoopWorkflow": EarlyReturnLoopWorkflow,
    "KwOnlyLocationWorkflow": KwOnlyLocationWorkflow,
    "SpreadEmptyCollectionWorkflow": SpreadEmptyCollectionWorkflow,
    "ManyActionsWorkflow": ManyActionsWorkflow,
    "LoopingSleepWorkflow": LoopingSleepWorkflow,
    "NoOpWorkflow": NoOpWorkflow,
}


class ScheduleRequest(BaseModel):
    workflow_name: str = Field(description="Name of the workflow to schedule")
    schedule_type: Literal["cron", "interval"] = Field(description="Type of schedule")
    cron_expression: Optional[str] = Field(
        default=None,
        description="Cron expression (e.g., '*/5 * * * *' for every 5 minutes)",
    )
    interval_seconds: Optional[int] = Field(
        default=None, ge=10, description="Interval in seconds (minimum 10)"
    )
    inputs: Optional[dict] = Field(
        default=None, description="Input arguments to pass to each scheduled run"
    )


class ScheduleResponse(BaseModel):
    success: bool
    schedule_id: Optional[str] = None
    message: str


class ScheduleActionRequest(BaseModel):
    workflow_name: str = Field(description="Name of the workflow")


class ScheduleActionResponse(BaseModel):
    success: bool
    message: str


class BatchRunRequest(BaseModel):
    workflow_name: str = Field(description="Name of the workflow to enqueue")
    count: int = Field(
        default=1,
        ge=1,
        description="Number of instances to enqueue when inputs_list is not provided",
    )
    inputs: Optional[dict] = Field(
        default=None,
        description="Base inputs to reuse for all instances when inputs_list is not provided",
    )
    inputs_list: Optional[list[dict]] = Field(
        default=None,
        description="Per-instance inputs; overrides count/inputs if provided",
    )
    batch_size: int = Field(
        default=500,
        ge=1,
        le=10000,
        description="Number of instances to insert per database batch",
    )
    priority: Optional[int] = Field(
        default=None,
        description="Optional priority for queued instances (higher runs first)",
    )
    include_instance_ids: bool = Field(
        default=False,
        description="Include instance IDs in SSE batch events",
    )


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def _required_workflow_inputs(workflow_cls: type) -> set[str]:
    try:
        run_impl = workflow_cls.__workflow_run_impl__
    except AttributeError:
        run_impl = workflow_cls.run
    signature = inspect.signature(run_impl)
    required = set()
    for name, param in list(signature.parameters.items())[1:]:
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            continue
        if param.default is inspect._empty:
            required.add(name)
    return required


def _missing_input_keys(required: set[str], inputs: dict) -> list[str]:
    return [key for key in sorted(required) if inputs.get(key) is None]


@app.post("/api/schedule", response_model=ScheduleResponse)
async def register_schedule(payload: ScheduleRequest) -> ScheduleResponse:
    """Register a workflow to run on a schedule."""
    workflow_cls = WORKFLOW_REGISTRY.get(payload.workflow_name)
    if not workflow_cls:
        return ScheduleResponse(
            success=False,
            message=f"Unknown workflow: {payload.workflow_name}",
        )

    try:
        if payload.schedule_type == "cron":
            if not payload.cron_expression:
                return ScheduleResponse(
                    success=False,
                    message="Cron expression required for cron schedule type",
                )
            schedule = payload.cron_expression
        else:
            if not payload.interval_seconds:
                return ScheduleResponse(
                    success=False,
                    message="Interval seconds required for interval schedule type",
                )
            schedule = timedelta(seconds=payload.interval_seconds)

        schedule_id = await schedule_workflow(
            workflow_cls,
            schedule_name=payload.workflow_name,
            schedule=schedule,
            inputs=payload.inputs,
        )
        return ScheduleResponse(
            success=True,
            schedule_id=schedule_id,
            message=f"Schedule registered for {payload.workflow_name}",
        )
    except Exception as e:
        return ScheduleResponse(success=False, message=str(e))


@app.post("/api/batch-run")
async def run_batch_workflow(payload: BatchRunRequest) -> StreamingResponse:
    """Queue a batch of workflow instances and stream progress via SSE."""
    workflow_cls = WORKFLOW_REGISTRY.get(payload.workflow_name)
    if not workflow_cls:
        raise HTTPException(
            status_code=404, detail=f"Unknown workflow: {payload.workflow_name}"
        )

    inputs_list = payload.inputs_list
    if inputs_list is not None and len(inputs_list) == 0:
        raise HTTPException(status_code=400, detail="inputs_list must not be empty")

    total = len(inputs_list) if inputs_list is not None else payload.count
    if total < 1:
        raise HTTPException(status_code=400, detail="count must be >= 1")

    base_inputs = dict(payload.inputs or {})
    required_keys = _required_workflow_inputs(workflow_cls)
    if inputs_list is not None:
        for idx, inputs in enumerate(inputs_list):
            missing = _missing_input_keys(required_keys, inputs)
            if missing:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"inputs_list[{idx}] missing required keys: "
                        f"{', '.join(missing)}"
                    ),
                )
    else:
        missing = _missing_input_keys(required_keys, base_inputs)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"inputs missing required keys: {', '.join(missing)}",
            )

    async def event_stream() -> AsyncIterator[str]:
        start_time = time.perf_counter()
        try:
            yield _sse_event(
                "start",
                {
                    "workflow_name": payload.workflow_name,
                    "total": total,
                    "batch_size": payload.batch_size,
                },
            )

            registration = workflow_cls._build_registration_payload(
                priority=payload.priority
            )
            if inputs_list is not None:
                batch_inputs = [
                    workflow_cls._build_initial_context((), inputs)
                    for inputs in inputs_list
                ]
                base_inputs_message = None
            else:
                batch_inputs = None
                base_inputs_message = (
                    workflow_cls._build_initial_context((), base_inputs)
                    if payload.inputs is not None
                    else None
                )

            batch_result = await bridge.run_instances_batch(
                registration.SerializeToString(),
                count=total,
                inputs=base_inputs_message,
                inputs_list=batch_inputs,
                batch_size=payload.batch_size,
                include_instance_ids=payload.include_instance_ids,
            )
            workflow_cls._workflow_version_id = batch_result.workflow_version_id
            elapsed_ms = int((time.perf_counter() - start_time) * 1000)
            yield _sse_event(
                "complete",
                {
                    "workflow_version_id": batch_result.workflow_version_id,
                    "queued": batch_result.queued,
                    "total": total,
                    "elapsed_ms": elapsed_ms,
                    "instance_ids": batch_result.workflow_instance_ids
                    if payload.include_instance_ids
                    else None,
                },
            )
        except Exception as exc:  # pragma: no cover - streaming errors
            yield _sse_event("error", {"message": str(exc)})

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/api/schedule/pause", response_model=ScheduleActionResponse)
async def pause_workflow_schedule(
    payload: ScheduleActionRequest,
) -> ScheduleActionResponse:
    """Pause a workflow's schedule."""
    workflow_cls = WORKFLOW_REGISTRY.get(payload.workflow_name)
    if not workflow_cls:
        return ScheduleActionResponse(
            success=False,
            message=f"Unknown workflow: {payload.workflow_name}",
        )

    try:
        result = await pause_schedule(workflow_cls)
        if result:
            return ScheduleActionResponse(
                success=True,
                message=f"Schedule paused for {payload.workflow_name}",
            )
        return ScheduleActionResponse(
            success=False,
            message=f"No active schedule found for {payload.workflow_name}",
        )
    except Exception as e:
        return ScheduleActionResponse(success=False, message=str(e))


@app.post("/api/schedule/resume", response_model=ScheduleActionResponse)
async def resume_workflow_schedule(
    payload: ScheduleActionRequest,
) -> ScheduleActionResponse:
    """Resume a paused workflow schedule."""
    workflow_cls = WORKFLOW_REGISTRY.get(payload.workflow_name)
    if not workflow_cls:
        return ScheduleActionResponse(
            success=False,
            message=f"Unknown workflow: {payload.workflow_name}",
        )

    try:
        result = await resume_schedule(workflow_cls)
        if result:
            return ScheduleActionResponse(
                success=True,
                message=f"Schedule resumed for {payload.workflow_name}",
            )
        return ScheduleActionResponse(
            success=False,
            message=f"No paused schedule found for {payload.workflow_name}",
        )
    except Exception as e:
        return ScheduleActionResponse(success=False, message=str(e))


@app.post("/api/schedule/delete", response_model=ScheduleActionResponse)
async def delete_workflow_schedule(
    payload: ScheduleActionRequest,
) -> ScheduleActionResponse:
    """Delete a workflow's schedule."""
    workflow_cls = WORKFLOW_REGISTRY.get(payload.workflow_name)
    if not workflow_cls:
        return ScheduleActionResponse(
            success=False,
            message=f"Unknown workflow: {payload.workflow_name}",
        )

    try:
        result = await delete_schedule(workflow_cls)
        if result:
            return ScheduleActionResponse(
                success=True,
                message=f"Schedule deleted for {payload.workflow_name}",
            )
        return ScheduleActionResponse(
            success=False,
            message=f"No schedule found for {payload.workflow_name}",
        )
    except Exception as e:
        return ScheduleActionResponse(success=False, message=str(e))


# =============================================================================
# Database Reset (Development Only)
# =============================================================================


class ResetResponse(BaseModel):
    success: bool
    message: str


@app.post("/api/reset", response_model=ResetResponse)
async def reset_database() -> ResetResponse:
    """Reset workflow-related tables for a clean slate. Development use only."""
    database_url = os.environ.get("WAYMARK_DATABASE_URL")
    if not database_url:
        return ResetResponse(
            success=False, message="WAYMARK_DATABASE_URL not configured"
        )

    try:
        conn = await asyncpg.connect(database_url)
        try:
            # Truncate all data.
            # See also src/backends/postgres/test_helpers.rs
            await conn.execute("""
                TRUNCATE runner_actions_done,
                    queued_instances,
                    runner_instances,
                    workflow_versions,
                    workflow_schedules,
                    worker_status
                RESTART IDENTITY CASCADE
            """)
            return ResetResponse(success=True, message="All workflow data cleared")
        finally:
            await conn.close()
    except Exception as e:
        return ResetResponse(success=False, message=str(e))
