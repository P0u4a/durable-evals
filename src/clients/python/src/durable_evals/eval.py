from __future__ import annotations

import uuid
from pathlib import Path

from .runtime import RuntimeClient


class DurableEval:
    def __init__(self, *, run_id: str | None = None, storage_dir: str | Path = ".durable"):
        self.run_id = run_id or str(uuid.uuid4())
        self._runtime = RuntimeClient.ensure_started(Path(storage_dir))


Eval = DurableEval
