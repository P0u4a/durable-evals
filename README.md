# Durable Evals

A durable eval harness for performing long-running evals on agents, recovering gracefully from intermittent or transient errors.

## Python Client

```python
from durable_evals import DurableEval, step


class MyEval(DurableEval):
    @step
    async def fetch_cases(self):
        return [{"id": "case-1"}]

    @step
    async def run_agent(self, test_case):
        return {"case_id": test_case["id"], "answer": "ok"}

    async def run(self):
        cases = await self.fetch_cases()
        results = []
        for case in cases:
            results.append(await self.run_agent(case))
        return results
```

## Node Client

```js
import { DurableEval } from "durable-evals";

const myEval = new DurableEval();

const fetchCases = myEval.addStep("fetchCases", async () => [{ id: "case-1" }]);
const runAgent = myEval.addStep("runAgent", async (testCase) => ({
  case_id: testCase.id,
  answer: "ok",
}));

const cases = await fetchCases();
const results = [];
for await (const result of cases.map((testCase) => runAgent(testCase))) {
  results.push(result);
}
```
