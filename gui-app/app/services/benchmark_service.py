from collections import defaultdict
from datetime import datetime

from app.config.constants import DATA_DIR
from app.models.benchmark_models import BenchmarkRecord, BenchmarkRun
from app.models.server_config import ServerConfig
from app.services.query_service import QueryService
from app.services.results_repository import ResultsRepository
from app.services.server_service import ServerService


class BenchmarkService:
    """Runs the same query workload across several startup policies."""

    def __init__(
        self,
        server_service: ServerService,
        query_service_factory,
        results_repository: ResultsRepository,
    ):
        self.server_service = server_service
        self.query_service_factory = query_service_factory
        self.results_repository = results_repository

    def run(
        self,
        queries: list[str],
        policies: list[str],
        frames: int,
        on_trace_ready=None,
    ) -> tuple[BenchmarkRun, tuple]:
        name = datetime.now().strftime("benchmark_%Y%m%d_%H%M%S")
        benchmark_run = BenchmarkRun(name=name)

        for policy in policies:
            if self.server_service.is_running():
                self.server_service.stop()

            if policy == "opt":
                self._run_opt(benchmark_run, queries, frames, on_trace_ready=on_trace_ready)
            else:
                config = ServerConfig(policy=policy, frames=frames)
                self.server_service.start(config)
                query_service: QueryService = self.query_service_factory(config)
                trace_events = self._record_policy(benchmark_run, query_service, queries, policy)
                benchmark_run.policy_traces[policy] = trace_events
                benchmark_run.trace_frame_counts[policy] = self._frame_count_from_trace(trace_events, frames)
                if on_trace_ready is not None:
                    on_trace_ready(policy, trace_events, benchmark_run.trace_frame_counts[policy])

        saved_paths = self.results_repository.save(benchmark_run)
        return benchmark_run, saved_paths

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _record_policy(
        self,
        benchmark_run: BenchmarkRun,
        query_service: QueryService,
        queries: list[str],
        policy: str,
    ) -> list[dict]:
        query_service.trace_start()
        try:
            for query in queries:
                if not query.strip():
                    continue
                result = query_service.execute(query)
                benchmark_run.records.append(
                    BenchmarkRecord(
                        policy=policy,
                        query=query,
                        hits=result.metrics.hits,
                        misses=result.metrics.misses,
                        evictions=result.metrics.evictions,
                        hit_rate=result.metrics.hit_rate,
                        elapsed_time=result.metrics.elapsed_time,
                        ok=result.error is None,
                        error=result.error or "",
                    )
                )
            return query_service.trace_stop()
        except Exception:
            query_service.trace_clear()
            raise

    def _run_opt(
        self,
        benchmark_run: BenchmarkRun,
        queries: list[str],
        frames: int,
        on_trace_ready=None,
    ) -> None:
        # Phase 1: collect trace with nocache (all misses → complete access sequence)
        trace_config = ServerConfig(policy="nocache", frames=frames)
        self.server_service.start(trace_config)
        trace_qs: QueryService = self.query_service_factory(trace_config)

        trace_qs.trace_start()
        for query in queries:
            if query.strip():
                trace_qs.execute(query)
        trace_events = trace_qs.trace_stop()

        self.server_service.stop()

        # Save trace: one "table:page_id" per line
        opt_trace_path = DATA_DIR / "opt_trace.txt"
        with open(opt_trace_path, "w") as f:
            for ev in trace_events:
                f.write(f"{ev['table']}:{ev['page_id']}\n")

        # Phase 2: restart with opt policy; server reads opt_trace.txt on startup
        opt_config = ServerConfig(policy="opt", frames=frames)
        self.server_service.start(opt_config)
        opt_qs: QueryService = self.query_service_factory(opt_config)
        trace_events = self._record_policy(benchmark_run, opt_qs, queries, "opt")
        benchmark_run.policy_traces["opt"] = trace_events
        benchmark_run.trace_frame_counts["opt"] = self._frame_count_from_trace(trace_events, frames)
        if on_trace_ready is not None:
            on_trace_ready("opt", trace_events, benchmark_run.trace_frame_counts["opt"])

    @staticmethod
    def _frame_count_from_trace(trace_events: list[dict], default: int) -> int:
        if not trace_events:
            return default
        first_event = trace_events[0]
        frames = first_event.get("frames", [])
        return len(frames) if frames else default

    # -------------------------------------------------------------------------

    @staticmethod
    def summarize_by_policy(benchmark_run: BenchmarkRun) -> dict[str, dict[str, float]]:
        totals: dict[str, dict[str, float]] = defaultdict(
            lambda: {
                "hits": 0.0,
                "misses": 0.0,
                "evictions": 0.0,
                "elapsed_time": 0.0,
                "count": 0.0,
            }
        )
        for record in benchmark_run.records:
            bucket = totals[record.policy]
            bucket["hits"] += record.hits
            bucket["misses"] += record.misses
            bucket["evictions"] += record.evictions
            bucket["elapsed_time"] += record.elapsed_time
            bucket["count"] += 1

        summary: dict[str, dict[str, float]] = {}
        for policy, bucket in totals.items():
            total_accesses = bucket["hits"] + bucket["misses"]
            summary[policy] = {
                "hits": bucket["hits"],
                "misses": bucket["misses"],
                "evictions": bucket["evictions"],
                "elapsed_time": bucket["elapsed_time"],
                "hit_rate": (bucket["hits"] / total_accesses) if total_accesses else 0.0,
            }
        return summary
