import io
import sys
import time
from pathlib import Path

from app.models.query_models import QueryExecutionResult, QueryMetrics


class SQLEngineAdapter:
    """Encapsulates all direct interaction with modules under sql-engine/."""

    def __init__(self, repo_root: Path, host: str, port: int):
        self.repo_root = repo_root
        self.host = host
        self.port = port
        self._bootstrap_sql_engine()

        self.parser = self.get_parser()
        self.planner = self.QueryPlanner()
        self.storage = self._build_tracking_storage(host=host, port=port)
        self.executor = self.QueryExecutor(self.storage)

    def _bootstrap_sql_engine(self) -> None:
        sql_engine_path = self.repo_root / "sql-engine"
        sql_engine_str = str(sql_engine_path)
        if sql_engine_str not in sys.path:
            sys.path.insert(0, sql_engine_str)

        from parser import get_parser  # type: ignore
        from planner import QueryPlanner  # type: ignore
        from executor import QueryExecutor  # type: ignore
        from storage_server import ServerStorage  # type: ignore

        self.get_parser = get_parser
        self.QueryPlanner = QueryPlanner
        self.QueryExecutor = QueryExecutor
        self.ServerStorage = ServerStorage

    def _build_tracking_storage(self, host: str, port: int):
        base_cls = self.ServerStorage

        class TrackingServerStorage(base_cls):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.last_metrics: dict[str, float | int] = {}

            def _read_response(self, sock, columns=None):
                response = super()._read_response(sock, columns=columns)
                self.last_metrics = response.get("metrics", {}) or {}
                return response

            def ping_with_metrics(self) -> dict[str, float | int]:
                sock = self._connect()
                try:
                    self._send(sock, "PING")
                    response = self._read_response(sock)
                finally:
                    sock.close()
                if response["status"] != "OK":
                    raise RuntimeError(response.get("err_line", "PING failed"))
                return dict(self.last_metrics)

        return TrackingServerStorage(host=host, port=port)

    def reset_metrics(self) -> dict[str, float | int]:
        """Send RESET_METRICS to server and return the zeroed metrics dict."""
        return self.storage.reset_metrics()

    def execute_query(self, query: str) -> QueryExecutionResult:
        stripped = query.strip()
        if not stripped:
            return QueryExecutionResult(query=query, error="Query is empty.")

        if stripped.endswith(";"):
            stripped = stripped[:-1]

        before = self.storage.ping_with_metrics()
        started_at = time.perf_counter()
        old_stdout = sys.stdout

        try:
            sys.stdout = io.StringIO()
            ast = self.parser.parse(stripped)
        finally:
            captured = sys.stdout.getvalue()
            sys.stdout = old_stdout

        if ast is None:
            error = captured.strip() or "Syntax error"
            return QueryExecutionResult(query=query, error=error)

        try:
            plan = self.planner.plan(ast)
            rows = self.executor.execute(plan)
            error = None
        except Exception as exc:  # pragma: no cover - error path depends on runtime data
            rows = []
            error = str(exc)

        elapsed = time.perf_counter() - started_at
        after = self.storage.ping_with_metrics()
        metrics = self._delta_metrics(before, after, elapsed)
        total_metrics = self._absolute_metrics(after, elapsed)

        return QueryExecutionResult(
            query=query,
            rows=rows if isinstance(rows, list) else [],
            metrics=metrics,
            total_metrics=total_metrics,
            error=error,
        )

    @staticmethod
    def _delta_metrics(
        before: dict[str, float | int],
        after: dict[str, float | int],
        elapsed_time: float,
    ) -> QueryMetrics:
        hits = int(after.get("hits", 0)) - int(before.get("hits", 0))
        misses = int(after.get("misses", 0)) - int(before.get("misses", 0))
        evictions = int(after.get("evictions", 0)) - int(before.get("evictions", 0))
        total = hits + misses
        hit_rate = float(hits / total) if total else 0.0
        return QueryMetrics(
            hits=hits,
            misses=misses,
            evictions=evictions,
            hit_rate=hit_rate,
            elapsed_time=elapsed_time,
        )

    @staticmethod
    def _absolute_metrics(
        snapshot: dict[str, float | int],
        elapsed_time: float,
    ) -> QueryMetrics:
        return QueryMetrics(
            hits=int(snapshot.get("hits", 0)),
            misses=int(snapshot.get("misses", 0)),
            evictions=int(snapshot.get("evictions", 0)),
            hit_rate=float(snapshot.get("hit_rate", 0.0)),
            elapsed_time=elapsed_time,
        )
