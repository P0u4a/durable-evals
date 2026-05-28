# Durable Evals

A durable eval harness for performing long-running evals on agents, recovering gracefully from intermittent or transient errors.

## Python Client

An `Eval` base class plus `@step` decorator. The decorator
records function start/end with a hidden local Rust server, so user code only needs
to annotate durable boundaries.

```python
from durable_evals import Eval, step


class MyEval(Eval):
    @step
    async def fetch_cases(self):
        return [{"id": "case-1"}]

    @step
    async def run_agent(self, cases):
        return [{"case_id": case["id"], "answer": "ok"} for case in cases]

    async def run(self):
        cases = await self.fetch_cases()
        return await self.run_agent(cases)
```

## TypeScript (NodeJS) Client

todo
