"""Tkinter GUI for Stage A fetch-only workflow."""

from __future__ import annotations

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from cleaner import clean_thread_posts
from fetcher import fetch_thread, normalize_thread_base_url
from parser import parse_thread_folder
from reporter import generate_technical_report
from scorer import score_thread_posts
from storage import default_threads_root, safe_thread_folder_name


class DistillerFetchApp(tk.Tk):
    """Small GUI for fetching raw DIYAudio thread pages."""

    def __init__(self) -> None:
        super().__init__()
        self.title("DIYAudio Thread Distiller")
        self.geometry("860x560")
        self.minsize(720, 460)

        self._messages: queue.Queue[tuple[str, object]] = queue.Queue()
        self._worker: threading.Thread | None = None

        self.url_var = tk.StringVar()
        self.output_var = tk.StringVar(value=str(default_threads_root()))
        self.force_var = tk.BooleanVar(value=False)
        self.progress_var = tk.IntVar(value=0)
        self.report_top_n_var = tk.IntVar(value=50)

        self._build_widgets()
        self.after(100, self._process_messages)

    def _build_widgets(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(4, weight=1)

        form = ttk.Frame(self, padding=12)
        form.grid(row=0, column=0, sticky="ew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="Thread URL").grid(row=0, column=0, sticky="w", padx=(0, 8))
        ttk.Entry(form, textvariable=self.url_var).grid(row=0, column=1, sticky="ew")

        ttk.Label(form, text="Output folder").grid(
            row=1, column=0, sticky="w", padx=(0, 8), pady=(8, 0)
        )
        ttk.Entry(form, textvariable=self.output_var).grid(
            row=1, column=1, sticky="ew", pady=(8, 0)
        )
        ttk.Button(form, text="Browse...", command=self._choose_output_folder).grid(
            row=1, column=2, sticky="e", padx=(8, 0), pady=(8, 0)
        )

        ttk.Label(form, text="Report top N").grid(
            row=2, column=0, sticky="w", padx=(0, 8), pady=(8, 0)
        )
        ttk.Spinbox(
            form,
            from_=1,
            to=500,
            textvariable=self.report_top_n_var,
            width=8,
        ).grid(row=2, column=1, sticky="w", pady=(8, 0))

        controls = ttk.Frame(self, padding=(12, 0, 12, 8))
        controls.grid(row=1, column=0, sticky="ew")
        controls.columnconfigure(5, weight=1)

        self.start_button = ttk.Button(controls, text="Start Fetch", command=self._start_fetch)
        self.start_button.grid(row=0, column=0, sticky="w")

        self.parse_button = ttk.Button(
            controls,
            text="Parse downloaded pages",
            command=self._start_parse,
        )
        self.parse_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.clean_button = ttk.Button(
            controls,
            text="Clean / Normalize Posts",
            command=self._start_clean,
        )
        self.clean_button.grid(row=0, column=2, sticky="w", padx=(8, 0))

        self.score_button = ttk.Button(
            controls,
            text="Score Posts",
            command=self._start_score,
        )
        self.score_button.grid(row=0, column=3, sticky="w", padx=(8, 0))

        self.report_button = ttk.Button(
            controls,
            text="Generate Report",
            command=self._start_report,
        )
        self.report_button.grid(row=0, column=4, sticky="w", padx=(8, 0))

        ttk.Checkbutton(
            controls,
            text="Force re-fetch",
            variable=self.force_var,
        ).grid(row=0, column=6, sticky="w", padx=(16, 0))

        self.progress = ttk.Progressbar(
            self,
            variable=self.progress_var,
            maximum=100,
            mode="determinate",
        )
        self.progress.grid(row=2, column=0, sticky="ew", padx=12, pady=(0, 8))

        self.status_label = ttk.Label(self, text="Ready")
        self.status_label.grid(row=3, column=0, sticky="ew", padx=12)

        log_frame = ttk.Frame(self, padding=12)
        log_frame.grid(row=4, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, wrap="word", height=14)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        self.log_text.configure(state="disabled")

        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def _choose_output_folder(self) -> None:
        selected = filedialog.askdirectory(
            title="Select output folder",
            initialdir=self.output_var.get() or str(Path.cwd()),
        )
        if selected:
            self.output_var.set(selected)

    def _start_fetch(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        thread_url = self.url_var.get().strip()
        if not thread_url:
            messagebox.showerror("Missing URL", "Enter a DIYAudio thread URL.")
            return

        output_root = Path(self.output_var.get()).expanduser()
        self.progress_var.set(0)
        self.status_label.configure(text="Starting...")
        self._set_work_buttons_state("disabled")
        self._append_log("Starting fetch.")

        self._worker = threading.Thread(
            target=self._run_fetch,
            args=(thread_url, output_root, self.force_var.get()),
            daemon=True,
        )
        self._worker.start()

    def _start_parse(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        try:
            thread_folder = self._resolve_thread_folder_for_parse()
        except ValueError as exc:
            messagebox.showerror("Cannot parse", str(exc))
            return

        self.progress_var.set(0)
        self.status_label.configure(text="Starting parse...")
        self._set_work_buttons_state("disabled")
        self._append_log("Starting parse.")

        self._worker = threading.Thread(
            target=self._run_parse,
            args=(thread_folder,),
            daemon=True,
        )
        self._worker.start()

    def _start_clean(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        try:
            thread_folder = self._resolve_thread_folder()
        except ValueError as exc:
            messagebox.showerror("Cannot clean", str(exc))
            return

        self.progress_var.set(0)
        self.status_label.configure(text="Starting clean...")
        self._set_work_buttons_state("disabled")
        self._append_log("Starting clean.")

        self._worker = threading.Thread(
            target=self._run_clean,
            args=(thread_folder,),
            daemon=True,
        )
        self._worker.start()

    def _start_score(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        try:
            thread_folder = self._resolve_thread_folder_for_score()
        except ValueError as exc:
            messagebox.showerror("Cannot score", str(exc))
            return

        self.progress_var.set(0)
        self.status_label.configure(text="Starting score...")
        self._set_work_buttons_state("disabled")
        self._append_log("Starting score.")

        self._worker = threading.Thread(
            target=self._run_score,
            args=(thread_folder,),
            daemon=True,
        )
        self._worker.start()

    def _start_report(self) -> None:
        if self._worker and self._worker.is_alive():
            return

        try:
            thread_folder = self._resolve_thread_folder_for_report()
            top_n = max(1, int(self.report_top_n_var.get()))
        except (ValueError, tk.TclError) as exc:
            messagebox.showerror("Cannot generate report", str(exc))
            return

        self.progress_var.set(0)
        self.status_label.configure(text="Starting report...")
        self._set_work_buttons_state("disabled")
        self._append_log(f"Starting report. Top N: {top_n}")

        self._worker = threading.Thread(
            target=self._run_report,
            args=(thread_folder, top_n),
            daemon=True,
        )
        self._worker.start()

    def _run_fetch(self, thread_url: str, output_root: Path, force_refetch: bool) -> None:
        try:
            manifest_path = fetch_thread(
                thread_url,
                output_root,
                force_refetch=force_refetch,
                log=lambda message: self._messages.put(("log", message)),
                progress=lambda current, total: self._messages.put(("progress", (current, total))),
            )
            self._messages.put(("done", manifest_path))
        except Exception as exc:
            self._messages.put(("error", exc))

    def _run_parse(self, thread_folder: Path) -> None:
        try:
            output_path = parse_thread_folder(
                thread_folder,
                log=lambda message: self._messages.put(("log", message)),
                progress=lambda current, total: self._messages.put(
                    ("parse_progress", (current, total))
                ),
            )
            self._messages.put(("parse_done", output_path))
        except Exception as exc:
            self._messages.put(("error", exc))

    def _run_clean(self, thread_folder: Path) -> None:
        try:
            output_path = clean_thread_posts(
                thread_folder,
                log=lambda message: self._messages.put(("log", message)),
                progress=lambda current, total: self._messages.put(
                    ("clean_progress", (current, total))
                ),
            )
            self._messages.put(("clean_done", output_path))
        except Exception as exc:
            self._messages.put(("error", exc))

    def _run_score(self, thread_folder: Path) -> None:
        try:
            output_path = score_thread_posts(
                thread_folder,
                log=lambda message: self._messages.put(("log", message)),
                progress=lambda current, total: self._messages.put(
                    ("score_progress", (current, total))
                ),
            )
            self._messages.put(("score_done", output_path))
        except Exception as exc:
            self._messages.put(("error", exc))

    def _run_report(self, thread_folder: Path, top_n: int) -> None:
        try:
            output_path = generate_technical_report(
                thread_folder,
                top_n=top_n,
                log=lambda message: self._messages.put(("log", message)),
                progress=lambda current, total: self._messages.put(
                    ("report_progress", (current, total))
                ),
            )
            self._messages.put(("report_done", output_path))
        except Exception as exc:
            self._messages.put(("error", exc))

    def _process_messages(self) -> None:
        while True:
            try:
                kind, payload = self._messages.get_nowait()
            except queue.Empty:
                break

            if kind == "log":
                self._append_log(str(payload))
            elif kind == "progress":
                current, total = payload
                percent = int((current / total) * 100) if total else 0
                self.progress_var.set(percent)
                self.status_label.configure(text=f"Fetched {current} of {total} page(s)")
            elif kind == "parse_progress":
                current, total = payload
                percent = int((current / total) * 100) if total else 0
                self.progress_var.set(percent)
                self.status_label.configure(text=f"Parsed {current} of {total} page(s)")
            elif kind == "clean_progress":
                current, total = payload
                percent = int((current / total) * 100) if total else 0
                self.progress_var.set(percent)
                self.status_label.configure(text=f"Cleaned {current} of {total} post(s)")
            elif kind == "score_progress":
                current, total = payload
                percent = int((current / total) * 100) if total else 0
                self.progress_var.set(percent)
                self.status_label.configure(text=f"Scored {current} of {total} post(s)")
            elif kind == "report_progress":
                current, total = payload
                percent = int((current / total) * 100) if total else 0
                self.progress_var.set(percent)
                self.status_label.configure(text=f"Report step {current} of {total}")
            elif kind == "done":
                self.progress_var.set(100)
                self.status_label.configure(text="Complete")
                self._append_log(f"Done. Manifest: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showinfo("Fetch complete", f"Manifest written:\n{payload}")
            elif kind == "parse_done":
                self.progress_var.set(100)
                self.status_label.configure(text="Parse complete")
                self._append_log(f"Done. Posts JSON: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showinfo("Parse complete", f"Posts written:\n{payload}")
            elif kind == "clean_done":
                self.progress_var.set(100)
                self.status_label.configure(text="Clean complete")
                self._append_log(f"Done. Clean posts JSON: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showinfo("Clean complete", f"Clean posts written:\n{payload}")
            elif kind == "score_done":
                self.progress_var.set(100)
                self.status_label.configure(text="Score complete")
                self._append_log(f"Done. Scored posts JSON: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showinfo("Score complete", f"Scored posts written:\n{payload}")
            elif kind == "report_done":
                self.progress_var.set(100)
                self.status_label.configure(text="Report complete")
                self._append_log(f"Done. Technical report: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showinfo("Report complete", f"Technical report written:\n{payload}")
            elif kind == "error":
                self.status_label.configure(text="Failed")
                self._append_log(f"Error: {payload}")
                self._set_work_buttons_state("normal")
                messagebox.showerror("Operation failed", str(payload))

        self.after(100, self._process_messages)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _set_work_buttons_state(self, state: str) -> None:
        self.start_button.configure(state=state)
        self.parse_button.configure(state=state)
        self.clean_button.configure(state=state)
        self.score_button.configure(state=state)
        self.report_button.configure(state=state)

    def _resolve_thread_folder_for_parse(self) -> Path:
        return self._resolve_thread_folder()

    def _resolve_thread_folder_for_score(self) -> Path:
        selected_path = Path(self.output_var.get()).expanduser()
        if (selected_path / "posts_clean.json").exists():
            return selected_path

        thread_url = self.url_var.get().strip()
        if thread_url:
            normalized_url = normalize_thread_base_url(thread_url)
            derived_thread_folder = selected_path / safe_thread_folder_name(normalized_url)
            if (derived_thread_folder / "posts_clean.json").exists():
                return derived_thread_folder

        raise ValueError(
            "Select a thread folder containing posts_clean.json, or enter the "
            "same thread URL with the output root used for fetching."
        )

    def _resolve_thread_folder_for_report(self) -> Path:
        selected_path = Path(self.output_var.get()).expanduser()
        if (selected_path / "posts_scored.json").exists():
            return selected_path

        thread_url = self.url_var.get().strip()
        if thread_url:
            normalized_url = normalize_thread_base_url(thread_url)
            derived_thread_folder = selected_path / safe_thread_folder_name(normalized_url)
            if (derived_thread_folder / "posts_scored.json").exists():
                return derived_thread_folder

        raise ValueError(
            "Select a thread folder containing posts_scored.json, or enter the "
            "same thread URL with the output root used for fetching."
        )

    def _resolve_thread_folder(self) -> Path:
        selected_path = Path(self.output_var.get()).expanduser()
        if (selected_path / "fetch_manifest.json").exists():
            return selected_path

        thread_url = self.url_var.get().strip()
        if thread_url:
            normalized_url = normalize_thread_base_url(thread_url)
            derived_thread_folder = selected_path / safe_thread_folder_name(normalized_url)
            if (derived_thread_folder / "fetch_manifest.json").exists():
                return derived_thread_folder

        raise ValueError(
            "Select a thread folder containing fetch_manifest.json, or enter the "
            "same thread URL with the output root used for fetching."
        )


def run_app() -> None:
    app = DistillerFetchApp()
    app.mainloop()
