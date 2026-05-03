from pathlib import Path

from app.adapters.sql_engine_adapter import SQLEngineAdapter
from app.models.query_models import QueryExecutionResult


class QueryService:
    """Executes SQL queries through the existing sql-engine pipeline."""

    def __init__(self, repo_root: Path, host: str, port: int):
        self.adapter = SQLEngineAdapter(repo_root=repo_root, host=host, port=port)

    def execute(self, query: str) -> QueryExecutionResult:
        return self.adapter.execute_query(query)

    def reset_metrics(self) -> dict[str, float | int]:
        return self.adapter.reset_metrics()
