from waymark import workflow, Workflow

@workflow
class SampleWorkflow(Workflow):
    async def run(self) -> str:
        return "sample"
