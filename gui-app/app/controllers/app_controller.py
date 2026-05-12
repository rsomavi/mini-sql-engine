from app.models.server_config import ServerConfig


class AppController:
    """Coordinates services for the GUI layer."""

    def __init__(self, server_service, query_service_factory, benchmark_service, telemetry_history_service):
        self.server_service = server_service
        self.query_service_factory = query_service_factory
        self.benchmark_service = benchmark_service
        self.telemetry_history_service = telemetry_history_service

    def start_server(self, config: ServerConfig):
        self.server_service.start(config)
        self.telemetry_history_service.clear_for_new_server()

    def stop_server(self):
        self.server_service.stop()

    def execute_query(self, config: ServerConfig, query: str, record_trace: bool = False):
        query_service = self.query_service_factory(config)
        if record_trace:
            query_service.trace_start()

        try:
            result = query_service.execute(query)
        except Exception:
            if record_trace:
                query_service.trace_clear()
            raise

        if record_trace:
            result.trace_recorded = True
            result.trace_events = query_service.trace_stop()

        result.total_metrics = self.telemetry_history_service.record_query(result)
        return result

    def run_benchmark(self, queries: list[str], policies: list[str], frames: int, on_trace_ready=None):
        return self.benchmark_service.run(
            queries=queries,
            policies=policies,
            frames=frames,
            on_trace_ready=on_trace_ready,
        )

    def reset_telemetry(self):
        return self.telemetry_history_service.reset()

    def reset_metrics(self, config: ServerConfig):
        """Send RESET_METRICS to the server, then reset Python-side telemetry totals."""
        query_service = self.query_service_factory(config)
        query_service.reset_metrics()
        return self.telemetry_history_service.reset()

    def telemetry_history_rows(self):
        return self.telemetry_history_service.history_rows()

    def clear_telemetry_history(self):
        return self.telemetry_history_service.clear_history()
