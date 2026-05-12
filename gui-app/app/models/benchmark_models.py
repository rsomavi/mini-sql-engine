from dataclasses import asdict, dataclass, field


@dataclass(slots=True)
class BenchmarkRecord:
    policy: str
    query: str
    hits: int
    misses: int
    evictions: int
    hit_rate: float
    elapsed_time: float
    ok: bool
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class BenchmarkRun:
    name: str
    records: list[BenchmarkRecord] = field(default_factory=list)
    policy_traces: dict[str, list[dict]] = field(default_factory=dict)
    trace_frame_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "records": [record.to_dict() for record in self.records],
        }
