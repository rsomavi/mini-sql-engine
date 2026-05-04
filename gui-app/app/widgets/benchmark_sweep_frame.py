import tkinter as tk
from tkinter import ttk

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

from app.config.constants import SUPPORTED_POLICIES
from app.config.theme import COLORS


class BenchmarkSweepFrame(tk.Frame):
    POLICY_COLORS = {
        "lru": "blue",
        "clock": "green",
        "nocache": "red",
        "opt": "purple",
    }
    POLICY_LABELS = {
        "lru": "LRU",
        "clock": "Clock",
        "nocache": "NoCache",
        "opt": "OPT",
    }

    def __init__(self, master, on_run_sweep):
        super().__init__(
            master,
            bg=COLORS["panel"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["border"],
            bd=0,
        )
        self.on_run_sweep = on_run_sweep
        self.frames_var = tk.StringVar(value="3, 8, 16, 32, 64")
        self.info_var = tk.StringVar(value="Run a sweep to compare hit rate by buffer pool size.")
        self.policy_vars = {policy: tk.BooleanVar(value=True) for policy in SUPPORTED_POLICIES}
        self.selected_policies = list(SUPPORTED_POLICIES)
        self.rows_by_frame = []

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=2)
        self.rowconfigure(2, weight=1)

        self._build_controls()
        self._build_chart()
        self._build_table()
        self._redraw_chart()

    def _build_controls(self):
        controls = tk.Frame(self, bg=COLORS["panel"])
        controls.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        controls.columnconfigure(1, weight=1)
        controls.columnconfigure(2, weight=0)

        tk.Label(
            controls,
            text="Buffer pool sizes (comma-separated):",
            bg=COLORS["panel"],
            fg=COLORS["text"],
            anchor="w",
            font=("TkDefaultFont", 10, "bold"),
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        entry = ttk.Entry(controls, textvariable=self.frames_var, style="Dashboard.TEntry")
        entry.grid(row=0, column=1, sticky="ew", padx=(10, 10), pady=(0, 8))

        self.run_button = ttk.Button(
            controls,
            text="Run Sweep",
            command=self._run_sweep,
            style="Primary.TButton",
        )
        self.run_button.grid(row=0, column=2, sticky="e", pady=(0, 8))

        policy_row = tk.Frame(controls, bg=COLORS["panel"])
        policy_row.grid(row=1, column=0, columnspan=3, sticky="ew")

        for index, policy in enumerate(SUPPORTED_POLICIES):
            ttk.Checkbutton(
                policy_row,
                text=policy,
                variable=self.policy_vars[policy],
            ).grid(row=0, column=index, sticky="w", padx=(0, 14))

        tk.Label(
            controls,
            textvariable=self.info_var,
            bg=COLORS["panel"],
            fg=COLORS["text_muted"],
            anchor="w",
            justify="left",
            font=("TkDefaultFont", 10),
        ).grid(row=2, column=0, columnspan=3, sticky="ew", pady=(8, 0))

    def _build_chart(self):
        chart_card = tk.Frame(
            self,
            bg=COLORS["panel_alt"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["border"],
            bd=0,
        )
        chart_card.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        chart_card.columnconfigure(0, weight=1)
        chart_card.rowconfigure(0, weight=1)

        self.figure = Figure(figsize=(6.8, 3.6), dpi=100)
        self.figure.patch.set_facecolor(COLORS["panel_alt"])
        self.axes = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=chart_card)
        self.canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")

    def _build_table(self):
        table_card = tk.Frame(
            self,
            bg=COLORS["panel_alt"],
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["border"],
            bd=0,
        )
        table_card.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        table_card.columnconfigure(0, weight=1)
        table_card.rowconfigure(1, weight=1)

        tk.Label(
            table_card,
            text="Sweep Results",
            bg=COLORS["panel_alt"],
            fg=COLORS["text"],
            anchor="w",
            font=("TkDefaultFont", 10, "bold"),
        ).grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))

        table_frame = tk.Frame(table_card, bg=COLORS["panel_alt"])
        table_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.table = ttk.Treeview(
            table_frame,
            show="headings",
            style="Dashboard.Treeview",
        )
        self.table.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.table.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.table.configure(yscrollcommand=scrollbar.set)
        self._configure_table_columns(self.selected_policies)

    def _run_sweep(self):
        frames = []
        for token in self.frames_var.get().split(","):
            token = token.strip()
            if not token:
                continue
            try:
                frames.append(int(token))
            except ValueError:
                continue

        policies = [policy for policy, variable in self.policy_vars.items() if variable.get()]
        self.on_run_sweep(frames, policies)

    def begin_sweep(self, frames: list[int], policies: list[str]):
        self.selected_policies = list(policies)
        self.rows_by_frame = []
        self._configure_table_columns(self.selected_policies)
        self._replace_table_rows()
        self._redraw_chart()
        self.set_running(True)
        self.info_var.set(f"Running sweep for frame sizes: {', '.join(str(frame) for frame in frames)}")

    def add_result(self, frames: int, summary: dict[str, dict[str, float]]):
        self.rows_by_frame.append({"frames": frames, "summary": summary})
        self.rows_by_frame.sort(key=lambda row: row["frames"])
        self._replace_table_rows()
        self._redraw_chart()

    def finish_sweep(self, csv_path):
        self.set_running(False)
        self.info_var.set(f"Saved sweep results to {csv_path.name}")

    def fail_sweep(self):
        self.set_running(False)
        self.info_var.set("Sweep failed.")

    def set_running(self, is_running: bool):
        self.run_button.configure(state="disabled" if is_running else "normal")

    def _configure_table_columns(self, policies: list[str]):
        columns = ["frames", *policies]
        self.table.configure(columns=columns)

        self.table.heading("frames", text="Frames")
        self.table.column("frames", width=90, minwidth=90, anchor="center")

        for policy in SUPPORTED_POLICIES:
            if policy not in columns:
                continue
            self.table.heading(policy, text=self.POLICY_LABELS[policy])
            self.table.column(policy, width=110, minwidth=110, anchor="center")

    def _replace_table_rows(self):
        for item in self.table.get_children():
            self.table.delete(item)

        for row in self.rows_by_frame:
            values = [row["frames"]]
            for policy in self.selected_policies:
                metric = row["summary"].get(policy, {}).get("hit_rate")
                values.append("" if metric is None else f"{metric:.3f}")
            self.table.insert("", "end", values=values)

    def _redraw_chart(self):
        self.axes.clear()
        self.axes.set_facecolor(COLORS["panel_alt"])
        self.axes.set_title("Hit Rate vs Buffer Pool Size", color=COLORS["text"], pad=12)
        self.axes.set_xlabel("Number of Frames", color=COLORS["text"])
        self.axes.set_ylabel("Hit Rate", color=COLORS["text"])
        self.axes.set_ylim(0.0, 1.0)
        self.axes.grid(True, color=COLORS["border"], alpha=0.45)
        self.axes.tick_params(colors=COLORS["text"])

        for spine in self.axes.spines.values():
            spine.set_color(COLORS["border"])

        if not self.rows_by_frame:
            self.axes.text(
                0.5,
                0.5,
                "Run a sweep to populate this chart.",
                color=COLORS["text_muted"],
                ha="center",
                va="center",
                transform=self.axes.transAxes,
            )
            self.canvas.draw_idle()
            return

        self.axes.set_xticks([row["frames"] for row in self.rows_by_frame])

        for policy in self.selected_policies:
            xs = []
            ys = []
            for row in self.rows_by_frame:
                if policy not in row["summary"]:
                    continue
                xs.append(row["frames"])
                ys.append(row["summary"][policy]["hit_rate"])

            if not xs:
                continue

            self.axes.plot(
                xs,
                ys,
                marker="o",
                linewidth=2,
                markersize=6,
                color=self.POLICY_COLORS[policy],
                label=self.POLICY_LABELS[policy],
            )

        handles, labels = self.axes.get_legend_handles_labels()
        if handles:
            self.axes.legend(
                handles,
                labels,
                facecolor=COLORS["panel_soft"],
                edgecolor=COLORS["border"],
                labelcolor=COLORS["text"],
                loc="best",
            )
        self.canvas.draw_idle()
