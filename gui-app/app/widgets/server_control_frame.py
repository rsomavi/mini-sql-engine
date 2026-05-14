import tkinter as tk
from tkinter import ttk

from app.config.constants import SUPPORTED_POLICIES
from app.config.theme import COLORS
from app.widgets.status_badge import StatusBadge


class ServerControlFrame(ttk.LabelFrame):
    def __init__(self, master, on_start, on_stop, start_collapsed: bool = False):
        super().__init__(master, text="Server Runtime", style="Panel.TLabelframe")
        self.on_start = on_start
        self.on_stop = on_stop
        self.is_collapsed = start_collapsed

        self.policy_var = tk.StringVar(value=SUPPORTED_POLICIES[0])
        self.frames_var = tk.StringVar(value="64")
        self.summary_var = tk.StringVar(value="Stopped")
        self.status_detail_var = tk.StringVar(value="Server offline.")
        self.configure(padding=12)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        summary_row = ttk.Frame(self, style="Panel.TFrame")
        summary_row.grid(row=0, column=0, sticky="ew")
        summary_row.columnconfigure(0, weight=1)
        summary_row.columnconfigure(1, weight=0)

        self.badge = StatusBadge(summary_row)
        self.badge.label.configure(width=0)
        self.badge.grid(row=0, column=0, sticky="w", padx=10, pady=(10, 10))

        self.toggle_button = ttk.Button(
            summary_row,
            text="",
            command=self._toggle_settings,
        )
        self.toggle_button.grid(row=0, column=1, sticky="e", padx=10, pady=(10, 10))

        self.details_frame = ttk.Frame(self, style="Panel.TFrame")
        self.details_frame.grid(row=1, column=0, sticky="ew")
        self.details_frame.columnconfigure(0, weight=1)
        self.details_frame.columnconfigure(1, weight=1)
        self.details_frame.columnconfigure(2, weight=1)

        ttk.Label(self.details_frame, text="Policy", style="Muted.TLabel").grid(
            row=0, column=0, sticky="w", padx=10, pady=(0, 6)
        )
        policy_box = ttk.Combobox(
            self.details_frame,
            textvariable=self.policy_var,
            values=SUPPORTED_POLICIES,
            state="readonly",
            style="Dashboard.TCombobox",
        )
        policy_box.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))

        ttk.Label(self.details_frame, text="Frames", style="Muted.TLabel").grid(
            row=0, column=1, sticky="w", padx=10, pady=(0, 6)
        )
        frames_entry = ttk.Entry(self.details_frame, textvariable=self.frames_var, style="Dashboard.TEntry")
        frames_entry.grid(row=1, column=1, sticky="ew", padx=10, pady=(0, 10))

        button_frame = ttk.Frame(self.details_frame, style="Panel.TFrame")
        button_frame.grid(row=1, column=2, sticky="e", padx=10, pady=(0, 10))
        button_frame.columnconfigure(0, weight=1)
        button_frame.columnconfigure(1, weight=1)

        ttk.Button(
            button_frame,
            text="Start Server",
            command=self._handle_start,
            style="Primary.TButton",
        ).grid(row=0, column=0, sticky="ew", padx=(0, 10))
        ttk.Button(
            button_frame,
            text="Stop Server",
            command=self.on_stop,
            style="Danger.TButton",
        ).grid(row=0, column=1, sticky="ew")

        self.status_detail = tk.Label(
            self.details_frame,
            textvariable=self.status_detail_var,
            bg=COLORS["panel"],
            fg=COLORS["text_muted"],
            justify="left",
            wraplength=720,
            anchor="w",
            font=("TkDefaultFont", 9),
        )
        self.status_detail.grid(row=2, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 10))

        self._sync_collapsed_state()

    def _handle_start(self):
        self.on_start(self.policy_var.get(), self.frames_var.get())

    def _toggle_settings(self):
        self.is_collapsed = not self.is_collapsed
        self._sync_collapsed_state()

    def _sync_collapsed_state(self):
        if self.is_collapsed:
            self.details_frame.grid_remove()
            self.toggle_button.configure(text="▼ Settings")
        else:
            self.details_frame.grid()
            self.toggle_button.configure(text="▲ Settings")

    def set_status(self, value: str):
        running = value.startswith("Running")
        if value == "Stopped":
            detail_text = "Server offline."
        elif "executing" in value.lower():
            detail_text = "Executing query against the active server."
        elif value.startswith("Running"):
            detail_text = value.replace("Running ", "", 1)
        elif "Benchmark" in value:
            detail_text = "Running benchmark workflow."
        else:
            detail_text = value
        self.summary_var.set(value)
        self.badge.set_state(running, self.summary_var.get())
        self.status_detail_var.set(detail_text)
