"""Integration test: workflow expression operators and builtins."""

from waymark import action, workflow
from waymark.workflow import Workflow


@action
async def format_ops(
    first: int,
    negated: int,
    floor_div: int,
    modulo: int,
    has_two: bool,
    no_four: bool,
    combined: bool,
    not_flag: bool,
    total: int,
    ranged_sum: int,
    indexed: int,
    dotted: int,
) -> str:
    return (
        f"first:{first}|neg:{negated}|floor:{floor_div}|mod:{modulo}"
        f"|has_two:{has_two}|no_four:{no_four}|combined:{combined}|not_flag:{not_flag}"
        f"|total:{total}|range:{ranged_sum}|indexed:{indexed}|dotted:{dotted}"
    )


@workflow
class ExpressionOpsWorkflow(Workflow):
    async def run(self) -> str:
        numbers = [1, 2, 3]
        first = numbers[0]
        has_two = 2 in numbers
        no_four = 4 not in numbers
        negated = -first
        floor_div = 7 // 3
        modulo = 7 % 3
        combined = (has_two and no_four) or False
        flag = False
        not_flag = not flag
        total = first + len(numbers)

        ranged_sum = 0
        for i in range(4):
            ranged_sum = ranged_sum + i

        info = {"indexed": numbers[1], "dotted": 9}
        indexed = info["indexed"]
        dotted = info.dotted

        return await format_ops(
            first=first,
            negated=negated,
            floor_div=floor_div,
            modulo=modulo,
            has_two=has_two,
            no_four=no_four,
            combined=combined,
            not_flag=not_flag,
            total=total,
            ranged_sum=ranged_sum,
            indexed=indexed,
            dotted=dotted,
        )
