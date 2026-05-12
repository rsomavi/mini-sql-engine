import tkinter as tk
from tkinter import ttk

from app.config.theme import COLORS


class CacheInspectorFrame(ttk.Frame):
    def __init__(self, master):
        super().__init__(master, style="Panel.TFrame")
        self.policy_var = tk.StringVar(value="")
        self.event_label_var = tk.StringVar(value="Event 0 / 0")
        self.speed_var = tk.DoubleVar(value=1.0)
        self.empty_var = tk.StringVar(value="No trace loaded.")

        self._policy_data: dict[str, dict] = {}
        self._current_policy = ""
        self._current_event_index = 0
        self._frame_cards: list[dict] = []
        self._play_job = None
        self._is_playing = False
        self._last_grid_columns = 0

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self.rowconfigure(2, weight=1)

        self._build_toolbar()
        self._build_grid_panel()
        self._build_log_panel()
        self._update_controls()

    def load_trace(self, events: list[dict], policy: str, n_frames: int):
        self._policy_data[policy] = {
            "events": events,
            "n_frames": n_frames,
        }

        policies = sorted(self._policy_data)
        self.policy_combo.configure(values=policies)
        self.policy_var.set(policy)
        self._current_policy = policy
        self._current_event_index = 0
        self._stop_playback()
        self._refresh_policy_view(rebuild_cards=True)

    def _build_toolbar(self):
        toolbar = ttk.Frame(self, style="Panel.TFrame")
        toolbar.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 0))
        toolbar.columnconfigure(8, weight=1)

        ttk.Label(toolbar, text="Cache Inspector", style="Heading.TLabel").grid(row=0, column=0, sticky="w")

        self.policy_combo = ttk.Combobox(
            toolbar,
            textvariable=self.policy_var,
            state="readonly",
            style="Dashboard.TCombobox",
            width=12,
        )
        self.policy_combo.grid(row=1, column=0, sticky="w", pady=(10, 0))
        self.policy_combo.bind("<<ComboboxSelected>>", self._on_policy_selected)

        tk.Label(
            toolbar,
            textvariable=self.event_label_var,
            bg=COLORS["panel"],
            fg=COLORS["text"],
            anchor="w",
            font=("TkDefaultFont", 10, "bold"),
        ).grid(row=1, column=1, sticky="w", padx=(12, 12), pady=(10, 0))

        self.first_button = ttk.Button(toolbar, text="|<<", command=self._jump_first)
        self.prev_button = ttk.Button(toolbar, text="<<", command=lambda: self._step(-1))
        self.play_button = ttk.Button(toolbar, text="Play", command=self._toggle_play)
        self.next_button = ttk.Button(toolbar, text=">>", command=lambda: self._step(1))
        self.last_button = ttk.Button(toolbar, text=">>|", command=self._jump_last)

        for index, button in enumerate(
            (self.first_button, self.prev_button, self.play_button, self.next_button, self.last_button),
            start=2,
        ):
            button.grid(row=1, column=index, sticky="w", padx=(0, 6), pady=(10, 0))

        tk.Label(
            toolbar,
            text="Speed",
            bg=COLORS["panel"],
            fg=COLORS["text_muted"],
        ).grid(row=1, column=7, sticky="w", padx=(6, 6), pady=(10, 0))

        self.speed_scale = ttk.Scale(
            toolbar,
            from_=0.1,
            to=10.0,
            variable=self.speed_var,
            orient="horizontal",
        )
        self.speed_scale.grid(row=1, column=8, sticky="ew", pady=(10, 0))

        self.speed_value_label = tk.Label(
            toolbar,
            text="1.0x",
            bg=COLORS["panel"],
            fg=COLORS["text"],
            width=6,
            anchor="e",
        )
        self.speed_value_label.grid(row=1, column=9, sticky="e", padx=(8, 0), pady=(10, 0))
        self.speed_var.trace_add("write", self._on_speed_changed)

    def _build_grid_panel(self):
        panel = ttk.LabelFrame(self, text="Buffer Pool Frames", style="Panel.TLabelframe")
        panel.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(0, weight=1)

        self.grid_canvas = tk.Canvas(
            panel,
            bg=COLORS["panel"],
            highlightthickness=0,
            bd=0,
        )
        self.grid_canvas.grid(row=0, column=0, sticky="nsew")

        grid_scrollbar = ttk.Scrollbar(panel, orient="vertical", command=self.grid_canvas.yview)
        grid_scrollbar.grid(row=0, column=1, sticky="ns")
        self.grid_canvas.configure(yscrollcommand=grid_scrollbar.set)

        self.grid_container = tk.Frame(self.grid_canvas, bg=COLORS["panel"])
        self.grid_window = self.grid_canvas.create_window((0, 0), window=self.grid_container, anchor="nw")
        self.grid_container.bind("<Configure>", self._sync_grid_scrollregion)
        self.grid_canvas.bind("<Configure>", self._on_grid_canvas_configure)

        self.grid_empty_label = tk.Label(
            self.grid_container,
            textvariable=self.empty_var,
            bg=COLORS["panel"],
            fg=COLORS["text_muted"],
            pady=24,
        )
        self.grid_empty_label.grid(row=0, column=0, sticky="w")

    def _build_log_panel(self):
        panel = ttk.LabelFrame(self, text="Event Log", style="Panel.TLabelframe")
        panel.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(0, weight=1)

        self.event_listbox = tk.Listbox(
            panel,
            bg=COLORS["panel_alt"],
            fg=COLORS["text"],
            selectbackground=COLORS["panel_soft"],
            selectforeground=COLORS["text"],
            activestyle="none",
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            relief="flat",
        )
        self.event_listbox.grid(row=0, column=0, sticky="nsew")
        self.event_listbox.bind("<<ListboxSelect>>", self._on_log_selected)

        scrollbar = ttk.Scrollbar(panel, orient="vertical", command=self.event_listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.event_listbox.configure(yscrollcommand=scrollbar.set)

    def _refresh_policy_view(self, rebuild_cards: bool):
        data = self._get_current_data()
        events = data["events"] if data else []
        n_frames = data["n_frames"] if data else 0

        if rebuild_cards:
            self._rebuild_frame_cards(n_frames if events else 0)
            self._populate_event_log(events)

        if not data:
            self.empty_var.set("No trace loaded.")
            self.event_label_var.set("Event 0 / 0")
            self._clear_frame_cards()
            self._update_controls()
            return

        if not events:
            self.empty_var.set(f"No events recorded for {self._current_policy}.")
            self.event_label_var.set("Event 0 / 0")
            self._clear_frame_cards()
            self._highlight_log_selection()
            self._update_controls()
            return

        self.empty_var.set("")
        self._current_event_index = max(0, min(self._current_event_index, len(events) - 1))
        self._render_current_event()
        self._update_controls()

    def _get_current_data(self):
        if not self._current_policy:
            return None
        return self._policy_data.get(self._current_policy)

    def _populate_event_log(self, events: list[dict]):
        self.event_listbox.delete(0, tk.END)
        for index, event in enumerate(events):
            self.event_listbox.insert(tk.END, self._format_event_line(index, event))

    def _format_event_line(self, index: int, event: dict) -> str:
        if event.get("evicted_frame", -1) >= 0:
            kind = "EVICT"
        else:
            kind = "HIT" if event.get("hit") else "MISS"
        return f"Event {index + 1} | {kind} | {event['table']}:{event['page_id']} -> frame {event['frame_id']}"

    def _render_current_event(self):
        data = self._get_current_data()
        if not data or not data["events"]:
            return

        events = data["events"]
        event = events[self._current_event_index]
        self.event_label_var.set(f"Event {self._current_event_index + 1} / {len(events)}")
        self._apply_frame_snapshot(event)
        self._highlight_log_selection()

    def _apply_frame_snapshot(self, event: dict):
        frames = event.get("frames", [])
        frame_lookup = {frame["frame_id"]: frame for frame in frames}
        for card in self._frame_cards:
            frame = frame_lookup.get(card["frame_id"])
            if frame is None:
                continue
            self._update_frame_card(card, frame, event)

    def _rebuild_frame_cards(self, n_frames: int):
        self._frame_cards = []
        for child in self.grid_container.winfo_children():
            child.destroy()

        if n_frames <= 0:
            self.grid_empty_label = tk.Label(
                self.grid_container,
                textvariable=self.empty_var,
                bg=COLORS["panel"],
                fg=COLORS["text_muted"],
                pady=24,
            )
            self.grid_empty_label.grid(row=0, column=0, sticky="w")
            return

        self.grid_empty_label = tk.Label(self.grid_container, text="", bg=COLORS["panel"])
        self.grid_empty_label.grid_forget()

        for frame_id in range(n_frames):
            card = self._create_frame_card(frame_id)
            self._frame_cards.append(card)

        self._layout_frame_cards(force=True)

    def _create_frame_card(self, frame_id: int) -> dict:
        outer = tk.Frame(
            self.grid_container,
            bg=COLORS["border"],
            highlightthickness=3,
            highlightbackground=COLORS["border"],
            highlightcolor=COLORS["border"],
            bd=0,
        )
        inner = tk.Frame(outer, bg=COLORS["panel_alt"], padx=10, pady=10)
        inner.pack(fill="both", expand=True)

        title = tk.Label(inner, text=f"Frame #{frame_id}", bg=COLORS["panel_alt"], fg=COLORS["text"], anchor="w")
        title.pack(fill="x")

        badge = tk.Label(inner, text="FREE", bg=COLORS["panel_soft"], fg=COLORS["text"], anchor="w", padx=8, pady=3)
        badge.pack(fill="x", pady=(8, 8))

        details = {}
        for key in ("table", "page", "dirty", "pin", "ref_bit", "last_access"):
            label = tk.Label(inner, bg=COLORS["panel_alt"], fg=COLORS["text_muted"], anchor="w")
            label.pack(fill="x")
            details[key] = label

        return {
            "frame_id": frame_id,
            "outer": outer,
            "inner": inner,
            "title": title,
            "badge": badge,
            "details": details,
        }

    def _layout_frame_cards(self, force: bool = False):
        if not self._frame_cards:
            return

        width = max(self.grid_canvas.winfo_width(), 1)
        columns = max(1, min(8, width // 190, len(self._frame_cards)))
        if not force and columns == self._last_grid_columns:
            return

        self._last_grid_columns = columns
        for index, card in enumerate(self._frame_cards):
            row = index // columns
            column = index % columns
            card["outer"].grid(row=row, column=column, sticky="nsew", padx=6, pady=6)

        for column in range(columns):
            self.grid_container.columnconfigure(column, weight=1)

    def _update_frame_card(self, card: dict, frame: dict, event: dict):
        state = frame["state"]
        if state == 0:
            badge_text = "FREE"
            bg = "#334155"
        elif state == 2:
            badge_text = "PINNED"
            bg = "#14532d"
        else:
            badge_text = "OCCUPIED"
            bg = "#1e3a8a"

        highlight = COLORS["border"]
        if frame["frame_id"] == event.get("evicted_frame", -1):
            highlight = COLORS["danger"]
        elif frame["frame_id"] == event.get("frame_id", -1):
            highlight = COLORS["success"] if event.get("hit") else COLORS["warning"]

        for widget in (card["outer"], card["inner"], card["title"]):
            widget.configure(bg=bg)
        card["badge"].configure(bg=COLORS["panel_soft"], fg=COLORS["text"], text=badge_text)
        card["outer"].configure(highlightbackground=highlight, highlightcolor=highlight)

        details = card["details"]
        details["table"].configure(text=f"table: {frame['table'] or '-'}", bg=bg)
        details["page"].configure(text=f"page: {frame['page_id']}", bg=bg)
        details["dirty"].configure(text=f"dirty: {'yes' if frame['dirty'] else 'no'}", bg=bg)
        details["pin"].configure(text=f"pin: {frame['pin_count']}", bg=bg)

        show_ref_bit = self._current_policy == "clock"
        show_last_access = self._current_policy in ("lru", "opt")
        details["ref_bit"].configure(
            text=f"ref_bit: {frame['ref_bit']}",
            bg=bg,
            fg=COLORS["text_muted"],
        )
        details["last_access"].configure(
            text=f"last_access: {frame['last_access']}",
            bg=bg,
            fg=COLORS["text_muted"],
        )

        if show_ref_bit:
            details["ref_bit"].pack(fill="x")
        else:
            details["ref_bit"].pack_forget()

        if show_last_access:
            details["last_access"].pack(fill="x")
        else:
            details["last_access"].pack_forget()

    def _clear_frame_cards(self):
        for card in self._frame_cards:
            card["outer"].destroy()
        self._frame_cards = []

    def _highlight_log_selection(self):
        self.event_listbox.selection_clear(0, tk.END)
        data = self._get_current_data()
        if not data or not data["events"]:
            return
        self.event_listbox.selection_set(self._current_event_index)
        self.event_listbox.activate(self._current_event_index)
        self.event_listbox.see(self._current_event_index)

    def _on_policy_selected(self, _event=None):
        selected = self.policy_var.get()
        if not selected or selected == self._current_policy:
            return
        self._current_policy = selected
        self._current_event_index = 0
        self._stop_playback()
        self._refresh_policy_view(rebuild_cards=True)

    def _on_log_selected(self, _event=None):
        selection = self.event_listbox.curselection()
        data = self._get_current_data()
        if not selection or not data or not data["events"]:
            return
        self._current_event_index = selection[0]
        self._stop_playback()
        self._render_current_event()
        self._update_controls()

    def _jump_first(self):
        if self._set_event_index(0):
            self._stop_playback()

    def _jump_last(self):
        data = self._get_current_data()
        if not data or not data["events"]:
            return
        if self._set_event_index(len(data["events"]) - 1):
            self._stop_playback()

    def _step(self, delta: int):
        if self._set_event_index(self._current_event_index + delta):
            self._stop_playback()

    def _set_event_index(self, index: int) -> bool:
        data = self._get_current_data()
        if not data or not data["events"]:
            return False
        bounded = max(0, min(index, len(data["events"]) - 1))
        if bounded == self._current_event_index and index == self._current_event_index:
            return False
        self._current_event_index = bounded
        self._render_current_event()
        self._update_controls()
        return True

    def _toggle_play(self):
        data = self._get_current_data()
        if not data or not data["events"]:
            return
        if self._is_playing:
            self._stop_playback()
            return
        self._is_playing = True
        self._update_controls()
        self._schedule_next_tick()

    def _schedule_next_tick(self):
        self._cancel_play_job()
        delay = max(50, int(1000 / max(self.speed_var.get(), 0.1)))
        self._play_job = self.after(delay, self._play_tick)

    def _play_tick(self):
        data = self._get_current_data()
        if not self._is_playing or not data or not data["events"]:
            self._stop_playback()
            return

        if self._current_event_index >= len(data["events"]) - 1:
            self._stop_playback()
            return

        self._current_event_index += 1
        self._render_current_event()
        self._update_controls()
        self._schedule_next_tick()

    def _stop_playback(self):
        self._is_playing = False
        self._cancel_play_job()
        self._update_controls()

    def _cancel_play_job(self):
        if self._play_job is not None:
            self.after_cancel(self._play_job)
            self._play_job = None

    def _update_controls(self):
        data = self._get_current_data()
        has_events = bool(data and data["events"])
        event_count = len(data["events"]) if has_events else 0
        at_first = not has_events or self._current_event_index <= 0
        at_last = not has_events or self._current_event_index >= event_count - 1

        combo_state = "readonly" if self._policy_data else "disabled"
        self.policy_combo.configure(state=combo_state)

        self.first_button.configure(state="disabled" if at_first else "normal")
        self.prev_button.configure(state="disabled" if at_first else "normal")
        self.next_button.configure(state="disabled" if at_last else "normal")
        self.last_button.configure(state="disabled" if at_last else "normal")
        self.play_button.configure(
            state="normal" if has_events and not at_last else "disabled",
            text="Pause" if self._is_playing else "Play",
        )

    def _on_speed_changed(self, *_args):
        self.speed_value_label.configure(text=f"{self.speed_var.get():.1f}x")
        if self._is_playing:
            self._schedule_next_tick()

    def _sync_grid_scrollregion(self, _event=None):
        self.grid_canvas.configure(scrollregion=self.grid_canvas.bbox("all"))

    def _on_grid_canvas_configure(self, event):
        self.grid_canvas.itemconfigure(self.grid_window, width=event.width)
        self._layout_frame_cards()
