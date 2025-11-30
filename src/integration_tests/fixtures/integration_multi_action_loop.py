import asyncio

from rappel import action, workflow
from rappel.workflow import Workflow


@action
async def load_order(order_id: str) -> dict:
    return {"id": order_id, "amount": 100}


@action
async def validate_order(order: dict) -> dict:
    return {"id": order["id"], "amount": order["amount"], "validated": True}


@action
async def process_payment(validated: dict) -> dict:
    return {
        "id": validated["id"],
        "amount": validated["amount"],
        "payment_id": f"PAY_{validated['id']}",
    }


@action
async def send_confirmation(payment: dict) -> str:
    return f"CONF_{payment['id']}_{payment['payment_id']}"


@action
async def summarize_confirmations(confirmations: list[str]) -> str:
    return "|".join(confirmations)


@workflow
class MultiActionLoopWorkflow(Workflow):
    """Test workflow with multiple actions per loop iteration."""

    async def run(self) -> str:
        # Load orders in parallel
        orders = await asyncio.gather(
            load_order(order_id="A"),
            load_order(order_id="B"),
            load_order(order_id="C"),
        )

        # Process each order with multiple actions per iteration
        confirmations = []
        for order in orders:
            validated = await validate_order(order=order)
            payment = await process_payment(validated=validated)
            confirmation = await send_confirmation(payment=payment)
            confirmations.append(confirmation)

        # Summarize results
        result = await summarize_confirmations(confirmations=confirmations)
        return result
