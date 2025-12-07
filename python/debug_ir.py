#!/usr/bin/env python3
"""Debug script to check IR structure for ForLoopWorkflow."""

import os
import sys

# Set test mode
os.environ["PYTEST_CURRENT_TEST"] = "true"

sys.path.insert(0, "src")
sys.path.insert(0, "../proto")

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def process_item(item: str) -> str:
    return item.upper()


@action
async def finalize_results(processed: list) -> str:
    return ",".join(processed)


@workflow
class ForLoopWorkflow(Workflow):
    async def run(self, items: list) -> str:
        processed = []
        for item in items:
            result = await process_item(item)
            processed.append(result)
        final = await finalize_results(processed)
        return final


if __name__ == "__main__":
    wf = ForLoopWorkflow()
    prog = wf.workflow_ir()
    if prog:
        print("=== PROGRAM FUNCTIONS ===")
        for fn in prog.functions:
            print(f"\nFunction: {fn.name}")
            if fn.io:
                print(f"  Inputs: {list(fn.io.inputs)}")
                print(f"  Outputs: {list(fn.io.outputs)}")
            if fn.body:
                print(f"  Statements: {len(fn.body.statements)}")
                for i, stmt in enumerate(fn.body.statements):
                    print(f"    [{i}] kind: {stmt.WhichOneof('kind')}")
                    if stmt.HasField("for_loop"):
                        fl = stmt.for_loop
                        print(f"        loop_vars: {list(fl.loop_vars)}")
                        if fl.iterable:
                            print(f"        iterable kind: {fl.iterable.WhichOneof('kind')}")
                            if fl.iterable.HasField("variable"):
                                print(f"        iterable var: {fl.iterable.variable.name}")
    else:
        print("No IR found")
