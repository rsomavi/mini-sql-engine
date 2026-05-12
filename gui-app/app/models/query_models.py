from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class QueryMetrics:
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    hit_rate: float = 0.0
    elapsed_time: float = 0.0


@dataclass(slots=True)
class QueryExecutionResult:
    query: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    metrics: QueryMetrics = field(default_factory=QueryMetrics)
    total_metrics: QueryMetrics = field(default_factory=QueryMetrics)
    trace_events: list[dict[str, Any]] = field(default_factory=list)
    trace_recorded: bool = False
    error: str | None = None
