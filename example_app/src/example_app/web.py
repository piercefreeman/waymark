"""FastAPI surface for the rappel example app."""

import os
from datetime import timedelta
from pathlib import Path
from typing import Literal, Optional

import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from rappel import delete_schedule, pause_schedule, resume_schedule, schedule_workflow

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
    LoopReturnRequest,
    LoopReturnResult,
    LoopReturnWorkflow,
    LoopProcessingWorkflow,
    LoopRequest,
    LoopResult,
    ParallelMathWorkflow,
    SequentialChainWorkflow,
    SleepRequest,
    SleepResult,
)

app = FastAPI(title="Rappel Example")

templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


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
# Return Inside Loop
# =============================================================================


@app.post("/api/loop-return", response_model=LoopReturnResult)
async def run_loop_return_workflow(payload: LoopReturnRequest) -> LoopReturnResult:
    """Run the early-return workflow demonstrating return inside a for-loop."""
    workflow = LoopReturnWorkflow()
    return await workflow.run(items=payload.items, needle=payload.needle)


# =============================================================================
# Error Handling (try/except)
# =============================================================================


@app.post("/api/error", response_model=ErrorResult)
async def run_error_workflow(payload: ErrorRequest) -> ErrorResult:
    """Run the error handling workflow demonstrating try/except."""
    workflow = ErrorHandlingWorkflow()
    return await workflow.run(should_fail=payload.should_fail)


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
# Early Return with Loop (if-return followed by for-loop)
# =============================================================================


class EarlyReturnLoopRequest(BaseModel):
    input_text: str = Field(description="Input text to parse. Use 'no_session:' prefix for early return path, or comma-separated items for loop path.")


@app.post("/api/early-return-loop", response_model=EarlyReturnLoopResult)
async def run_early_return_loop_workflow(payload: EarlyReturnLoopRequest) -> EarlyReturnLoopResult:
    """Run the early return + loop workflow demonstrating if-return followed by for-loop."""
    workflow = EarlyReturnLoopWorkflow()
    return await workflow.run(input_text=payload.input_text)


# =============================================================================
# Scheduled Workflows
# =============================================================================

# Mapping of workflow names to classes for dynamic lookup
WORKFLOW_REGISTRY = {
    "ParallelMathWorkflow": ParallelMathWorkflow,
    "SequentialChainWorkflow": SequentialChainWorkflow,
    "ConditionalBranchWorkflow": ConditionalBranchWorkflow,
    "LoopProcessingWorkflow": LoopProcessingWorkflow,
    "ErrorHandlingWorkflow": ErrorHandlingWorkflow,
    "DurableSleepWorkflow": DurableSleepWorkflow,
    "GuardFallbackWorkflow": GuardFallbackWorkflow,
    "EarlyReturnLoopWorkflow": EarlyReturnLoopWorkflow,
}


class ScheduleRequest(BaseModel):
    workflow_name: str = Field(description="Name of the workflow to schedule")
    schedule_type: Literal["cron", "interval"] = Field(description="Type of schedule")
    cron_expression: Optional[str] = Field(
        default=None, description="Cron expression (e.g., '*/5 * * * *' for every 5 minutes)"
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
            workflow_cls, schedule=schedule, inputs=payload.inputs
        )
        return ScheduleResponse(
            success=True,
            schedule_id=schedule_id,
            message=f"Schedule registered for {payload.workflow_name}",
        )
    except Exception as e:
        return ScheduleResponse(success=False, message=str(e))


@app.post("/api/schedule/pause", response_model=ScheduleActionResponse)
async def pause_workflow_schedule(payload: ScheduleActionRequest) -> ScheduleActionResponse:
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
async def resume_workflow_schedule(payload: ScheduleActionRequest) -> ScheduleActionResponse:
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
async def delete_workflow_schedule(payload: ScheduleActionRequest) -> ScheduleActionResponse:
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
    database_url = os.environ.get("RAPPEL_DATABASE_URL")
    if not database_url:
        return ResetResponse(success=False, message="RAPPEL_DATABASE_URL not configured")

    try:
        conn = await asyncpg.connect(database_url)
        try:
            # Delete in order respecting foreign key constraints
            await conn.execute("DELETE FROM daemon_action_ledger")
            await conn.execute("DELETE FROM workflow_instances")
            await conn.execute("DELETE FROM workflow_versions")
            return ResetResponse(success=True, message="All workflow data cleared")
        finally:
            await conn.close()
    except Exception as e:
        return ResetResponse(success=False, message=str(e))
