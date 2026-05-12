import tkinter as tk
from tkinter import ttk

from app.config.theme import COLORS
from app.widgets.results_table import ResultsTable


class QueryEditorFrame(ttk.Frame):
    def __init__(self, master, on_execute):
        super().__init__(master, style="Panel.TFrame")
        self.on_execute = on_execute
        self.error_var = tk.StringVar(value="")
        self.record_trace_var = tk.BooleanVar(value=False)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)

        header = ttk.Frame(self, style="Panel.TFrame")
        header.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text="SQL Workspace", style="Heading.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(
            header,
            text="Run ad-hoc queries against the live server and inspect the resulting rows.",
            style="Subheading.TLabel",
        ).grid(row=1, column=0, sticky="w", pady=(2, 0))

        toolbar = ttk.Frame(self, style="Panel.TFrame")
        toolbar.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        toolbar.columnconfigure(1, weight=1)

        ttk.Button(toolbar, text="Run Query", command=self._run_query, style="Primary.TButton").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Checkbutton(toolbar, text="Record trace", variable=self.record_trace_var).grid(
            row=0, column=1, sticky="w", padx=(12, 0)
        )
        ttk.Label(
            toolbar,
            text="The telemetry cards capture the last execution and cumulative totals.",
            style="Muted.TLabel",
        ).grid(row=0, column=2, sticky="w", padx=(12, 0))

        editor_frame = tk.Frame(
            self,
            bg=COLORS["panel_alt"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["border"],
            bd=0,
        )
        editor_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))
        editor_frame.columnconfigure(0, weight=1)

        self.query_text = tk.Text(
            editor_frame,
            height=8,
            wrap="word",
            bg=COLORS["panel_alt"],
            fg=COLORS["text"],
            insertbackground=COLORS["accent"],
            highlightthickness=0,
            relief="flat",
            padx=12,
            pady=12,
        )
        self.query_text.grid(row=0, column=0, sticky="nsew")
        self.query_text.insert("1.0", "SELECT * FROM users LIMIT 5;")

        self.error_label = tk.Label(
            self,
            textvariable=self.error_var,
            bg=COLORS["panel"],
            fg=COLORS["danger"],
            anchor="w",
            font=("TkDefaultFont", 10, "bold"),
        )
        self.error_label.grid(row=3, column=0, sticky="ew", padx=10, pady=(0, 4))

        self.results_table = ResultsTable(self)
        self.results_table.grid(row=4, column=0, sticky="nsew", padx=10, pady=(0, 10))
        self.rowconfigure(4, weight=1)

    def _run_query(self):
        query = self.query_text.get("1.0", "end").strip()
        self.on_execute(query, self.record_trace_var.get())

    def show_result(self, rows: list[dict], error: str | None):
        self.error_var.set(error or "")
        self.results_table.set_rows(rows)
