import csv
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from app.config.constants import APP_NAME, REPO_ROOT
from app.config.theme import apply_theme
from app.controllers.app_controller import AppController
from app.models.query_models import QueryMetrics
from app.models.server_config import ServerConfig
from app.services.benchmark_service import BenchmarkService
from app.services.query_service import QueryService
from app.services.results_repository import ResultsRepository
from app.services.server_service import ServerService
from app.services.telemetry_history_service import TelemetryHistoryService
from app.utils.paths import ensure_runtime_paths, results_dir_path
from app.widgets.benchmark_charts_frame import BenchmarkChartsFrame
from app.widgets.benchmark_frame import BenchmarkFrame
from app.widgets.benchmark_sweep_frame import BenchmarkSweepFrame
from app.widgets.cache_inspector_frame import CacheInspectorFrame
from app.widgets.metrics_frame import MetricsFrame
from app.widgets.query_editor_frame import QueryEditorFrame
from app.widgets.results_table import ResultsTable
from app.widgets.server_control_frame import ServerControlFrame


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        ensure_runtime_paths()
        apply_theme(self)

        self.title(APP_NAME)
        self.geometry("1400x920")
        self.minsize(1180, 760)

        self.server_service = ServerService()
        self.results_repository = ResultsRepository()
        self.telemetry_history_service = TelemetryHistoryService()

        def query_service_factory(config: ServerConfig) -> QueryService:
            return QueryService(REPO_ROOT, config.host, config.port)

        self.benchmark_service = BenchmarkService(
            server_service=self.server_service,
            query_service_factory=query_service_factory,
            results_repository=self.results_repository,
        )
        self.controller = AppController(
            server_service=self.server_service,
            query_service_factory=query_service_factory,
            benchmark_service=self.benchmark_service,
            telemetry_history_service=self.telemetry_history_service,
        )

        self.current_config = ServerConfig()
        self.query_service = query_service_factory(self.current_config)
        self._build_layout()

    def _build_layout(self):
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        shell = ttk.Frame(self, padding=10, style="Dashboard.TFrame")
        shell.grid(row=0, column=0, sticky="nsew")
        shell.columnconfigure(0, weight=1)
        shell.rowconfigure(1, weight=1)

        hero = ttk.Frame(shell, style="Dashboard.TFrame")
        hero.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        hero.columnconfigure(0, weight=1)

        ttk.Label(hero, text="minidbms Control Surface", style="Heading.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            hero,
            text="Operate the server, run queries, benchmark policies and inspect telemetry from dedicated tabs.",
            style="Subheading.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(4, 10))

        server_available = self.server_service.is_available(self.current_config.host, self.current_config.port)
        self.server_frame = ServerControlFrame(
            hero,
            on_start=self._on_start_server,
            on_stop=self._on_stop_server,
            start_collapsed=server_available,
        )
        self.server_frame.grid(row=2, column=0, sticky="ew")
        if server_available:
            self.server_frame.set_status(
                f"Running ({self.current_config.policy}, {self.current_config.frames} frames)"
            )
        else:
            self.server_frame.set_status("Stopped")

        self.main_notebook = ttk.Notebook(shell, style="Dashboard.TNotebook")
        self.main_notebook.grid(row=1, column=0, sticky="nsew")

        self.create_query_runner_tab()
        self.create_benchmark_tab()
        self.create_sweep_analysis_tab()
        self.create_cache_inspector_tab()
        self.create_telemetry_tab()
        self.refresh_telemetry_history()

    def create_query_runner_tab(self):
        tab = ttk.Frame(self.main_notebook, padding=10, style="Dashboard.TFrame")
        tab.columnconfigure(0, weight=3)
        tab.columnconfigure(1, weight=2)
        tab.rowconfigure(0, weight=1)

        query_panel = ttk.Frame(tab, style="Panel.TFrame")
        query_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        query_panel.columnconfigure(0, weight=1)
        query_panel.rowconfigure(0, weight=1)

        telemetry_panel = ttk.Frame(tab, style="Panel.TFrame")
        telemetry_panel.grid(row=0, column=1, sticky="nsew")
        telemetry_panel.columnconfigure(0, weight=1)
        telemetry_panel.rowconfigure(0, weight=1)

        self.query_frame = QueryEditorFrame(query_panel, on_execute=self._on_run_query)
        self.query_frame.grid(row=0, column=0, sticky="nsew")

        self.metrics_frame = MetricsFrame(telemetry_panel, on_reset=self._on_reset_telemetry)
        self.metrics_frame.grid(row=0, column=0, sticky="nsew")

        self.main_notebook.add(tab, text="Query Runner")

    def create_benchmark_tab(self):
        tab = ttk.Frame(self.main_notebook, padding=10, style="Dashboard.TFrame")
        tab.columnconfigure(0, weight=2)
        tab.columnconfigure(1, weight=3)
        tab.rowconfigure(0, weight=1)

        controls_panel = ttk.Frame(tab, style="Panel.TFrame")
        controls_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        controls_panel.columnconfigure(0, weight=1)
        controls_panel.rowconfigure(0, weight=1)

        charts_panel = ttk.Frame(tab, style="Panel.TFrame")
        charts_panel.grid(row=0, column=1, sticky="nsew")
        charts_panel.columnconfigure(0, weight=1)
        charts_panel.rowconfigure(0, weight=1)

        self.benchmark_frame = BenchmarkFrame(
            controls_panel,
            on_run_benchmark=self._on_run_benchmark,
        )
        self.benchmark_frame.grid(row=0, column=0, sticky="nsew")

        self.chart_frame = BenchmarkChartsFrame(charts_panel)
        self.chart_frame.grid(row=0, column=0, sticky="nsew")

        self.main_notebook.add(tab, text="Benchmark")

    def create_sweep_analysis_tab(self):
        tab = ttk.Frame(self.main_notebook, padding=10, style="Dashboard.TFrame")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)

        self.benchmark_sweep_frame = BenchmarkSweepFrame(
            tab,
            on_run_sweep=self._on_run_benchmark_sweep,
        )
        self.benchmark_sweep_frame.grid(row=0, column=0, sticky="nsew")

        self.main_notebook.add(tab, text="Sweep Analysis")

    def create_telemetry_tab(self):
        tab = ttk.Frame(self.main_notebook, padding=10, style="Dashboard.TFrame")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(tab, style="Panel.TFrame")
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        toolbar.columnconfigure(0, weight=1)
        toolbar.columnconfigure(1, weight=0)
        toolbar.columnconfigure(2, weight=0)

        ttk.Label(toolbar, text="Telemetry History", style="Heading.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Button(toolbar, text="Clear History", command=self._on_clear_history, style="Danger.TButton").grid(
            row=0, column=1, sticky="e", padx=(10, 10)
        )
        ttk.Button(toolbar, text="Export CSV", command=self._on_export_telemetry_csv, style="Primary.TButton").grid(
            row=0, column=2, sticky="e"
        )

        table_panel = ttk.Frame(tab, style="Panel.TFrame")
        table_panel.grid(row=1, column=0, sticky="nsew")
        table_panel.columnconfigure(0, weight=1)
        table_panel.rowconfigure(0, weight=1)

        self.telemetry_table = ResultsTable(
            table_panel,
            empty_message="No telemetry history yet.",
            tree_style="Telemetry.Treeview",
            heading_style="Telemetry.Treeview.Heading",
        )
        self.telemetry_table.grid(row=0, column=0, sticky="nsew")

        self.main_notebook.add(tab, text="Telemetry")

    def create_cache_inspector_tab(self):
        tab = ttk.Frame(self.main_notebook, padding=10, style="Dashboard.TFrame")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)

        self.cache_inspector = CacheInspectorFrame(tab)
        self.cache_inspector.grid(row=0, column=0, sticky="nsew")

        self.main_notebook.add(tab, text="Cache Inspector")

    def refresh_telemetry_history(self):
        self.telemetry_table.set_rows(self.controller.telemetry_history_rows())

    def _on_start_server(self, policy: str, frames_text: str):
        try:
            frames = int(frames_text)
        except ValueError:
            messagebox.showerror("Invalid frames", "Frames must be an integer.")
            return

        config = ServerConfig(policy=policy, frames=frames)
        try:
            self.controller.start_server(config)
        except Exception as exc:
            messagebox.showerror("Server startup failed", str(exc))
            self.server_frame.set_status("Stopped")
            return

        self.current_config = config
        self.query_service = QueryService(REPO_ROOT, config.host, config.port)
        self.server_frame.set_status(f"Running ({policy}, {frames} frames)")
        self.metrics_frame.update_metrics(QueryMetrics(), QueryMetrics())
        self.refresh_telemetry_history()

    def _on_stop_server(self):
        try:
            self.controller.stop_server()
        except Exception as exc:
            messagebox.showerror("Server shutdown failed", str(exc))
            return
        self.server_frame.set_status("Stopped")

    def _on_run_query(self, query: str, record_trace: bool):
        if not self.server_service.is_available(self.current_config.host, self.current_config.port):
            messagebox.showwarning("Server not running", "Start the server first.")
            return
        if not query.strip():
            messagebox.showwarning("Empty query", "Write a query first.")
            return
        self.server_frame.set_status(
            f"Running ({self.current_config.policy}, {self.current_config.frames} frames) · executing"
        )

        def worker():
            try:
                result = self.controller.execute_query(self.current_config, query, record_trace=record_trace)
                self.after(0, lambda: self._apply_query_result(result))
            except Exception as exc:
                self.after(0, lambda e=exc: self._handle_query_error(e))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_query_result(self, result):
        self.query_frame.show_result(result.rows, result.error)
        self.metrics_frame.update_metrics(result.metrics, result.total_metrics)
        if result.trace_recorded:
            self.cache_inspector.load_trace(
                events=result.trace_events,
                policy=self.current_config.policy,
                n_frames=(
                    len(result.trace_events[0].get("frames", []))
                    if result.trace_events
                    else self.current_config.frames
                ),
            )
        self.refresh_telemetry_history()
        self.server_frame.set_status(
            f"Running ({self.current_config.policy}, {self.current_config.frames} frames)"
        )

    def _handle_query_error(self, exc: Exception):
        self.query_frame.show_result([], str(exc))
        self.server_frame.set_status(
            f"Running ({self.current_config.policy}, {self.current_config.frames} frames)"
        )
        messagebox.showerror("Query execution failed", str(exc))

    def _on_reset_telemetry(self):
        if not self.server_service.is_available(self.current_config.host, self.current_config.port):
            messagebox.showwarning("Server not running", "Start the server before resetting metrics.")
            return
        try:
            totals = self.controller.reset_metrics(self.current_config)
        except Exception as exc:
            messagebox.showerror("Reset metrics failed", str(exc))
            return
        self.metrics_frame.update_metrics(QueryMetrics(), totals)
        self.refresh_telemetry_history()

    def _on_clear_history(self):
        self.controller.clear_telemetry_history()
        self.refresh_telemetry_history()

    def _on_export_telemetry_csv(self):
        rows = self.controller.telemetry_history_rows()
        if not rows:
            messagebox.showinfo("Telemetry export", "There is no telemetry history to export.")
            return

        default_name = f"telemetry_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        target = filedialog.asksaveasfilename(
            parent=self,
            title="Export telemetry history",
            initialdir=str(results_dir_path()),
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not target:
            return

        with open(target, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        self.server_frame.set_status(f"Running export saved to {target}")

    def _on_run_benchmark(self, queries: list[str], policies: list[str]):
        if not policies:
            messagebox.showwarning("No policies selected", "Select at least one policy.")
            return

        self.benchmark_frame.set_info("Running benchmark...")
        self.server_frame.set_status("Benchmark executing")

        def worker():
            try:
                benchmark_run, saved_paths = self.controller.run_benchmark(
                    queries=queries,
                    policies=policies,
                    frames=self.current_config.frames,
                    on_trace_ready=lambda policy, events, n_frames: self.after(
                        0,
                        lambda p=policy, ev=events, nf=n_frames: self.cache_inspector.load_trace(ev, p, nf),
                    ),
                )
                summary = self.benchmark_service.summarize_by_policy(benchmark_run)
                self.after(
                    0,
                    lambda: self._apply_benchmark_result(summary, saved_paths),
                )
            except Exception as exc:
                self.after(0, lambda e=exc: self._handle_benchmark_error(e))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_benchmark_result(self, summary: dict, saved_paths: tuple):
        json_path, csv_path = saved_paths
        self.benchmark_frame.set_info(
            f"Saved benchmark results to {json_path.name} and {csv_path.name}"
        )
        self.chart_frame.render_summary(summary)
        if self.server_service.is_available(self.current_config.host, self.current_config.port):
            self.server_frame.set_status("Running (benchmark complete)")
        else:
            self.server_frame.set_status("Stopped")

    def _handle_benchmark_error(self, exc: Exception):
        self.benchmark_frame.set_info("Benchmark failed.")
        self.server_frame.set_status("Stopped")
        messagebox.showerror("Benchmark failed", str(exc))

    def _on_run_benchmark_sweep(self, frames_list: list[int], policies: list[str]):
        if not policies:
            messagebox.showwarning("No policies selected", "Select at least one policy.")
            return
        if not frames_list:
            messagebox.showwarning("No frame sizes", "Enter at least one valid frame size.")
            return

        queries = [line.strip() for line in self.benchmark_frame.queries_text.get("1.0", "end").splitlines()]
        self.benchmark_sweep_frame.begin_sweep(frames_list, policies)
        self.server_frame.set_status("Sweep executing")

        def worker():
            sweep_rows = []
            try:
                for frames in frames_list:
                    benchmark_run, _ = self.controller.run_benchmark(
                        queries=queries,
                        policies=policies,
                        frames=frames,
                    )
                    summary = self.benchmark_service.summarize_by_policy(benchmark_run)
                    for policy, metrics in summary.items():
                        sweep_rows.append(
                            {
                                "frames": frames,
                                "policy": policy,
                                "hit_rate": metrics["hit_rate"],
                                "misses": metrics["misses"],
                                "evictions": metrics["evictions"],
                            }
                        )
                    self.after(
                        0,
                        lambda current_frames=frames, current_summary=summary: self.benchmark_sweep_frame.add_result(
                            current_frames,
                            current_summary,
                        ),
                    )

                csv_path = self.results_repository.save_sweep_csv(
                    name=f"sweep_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    rows=sweep_rows,
                )
                self.after(0, lambda: self._apply_sweep_result(csv_path))
            except Exception as exc:
                self.after(0, lambda e=exc: self._handle_sweep_error(e))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_sweep_result(self, csv_path):
        self.benchmark_sweep_frame.finish_sweep(csv_path)
        if self.server_service.is_available(self.current_config.host, self.current_config.port):
            self.server_frame.set_status("Running (sweep complete)")
        else:
            self.server_frame.set_status("Stopped")

    def _handle_sweep_error(self, exc: Exception):
        self.benchmark_sweep_frame.fail_sweep()
        self.server_frame.set_status("Stopped")
        messagebox.showerror("Sweep failed", str(exc))


def launch_app():
    window = MainWindow()
    window.mainloop()
