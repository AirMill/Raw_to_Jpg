import os
import sys
import json
import time
import threading
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import rawpy
import imageio.v2 as imageio
import piexif

import customtkinter as ctk
from tkinter import filedialog, messagebox


APP_NAME = "RAW → JPEG"
APP_VERSION = "1.0.0"

RAW_EXTS = {
    ".arw", ".srf", ".sr2",          # Sony
    ".cr2", ".cr3", ".crw",          # Canon
    ".nef", ".nrw",                  # Nikon
    ".raf",                          # Fuji
    ".dng",                          # DNG
    ".rw2",                          # Panasonic
    ".orf",                          # Olympus
    ".pef",                          # Pentax
}

# -----------------------------
# Settings / persistence
# -----------------------------

def app_data_dir() -> str:
    # Windows-friendly, but also works elsewhere.
    base = os.environ.get("APPDATA") or os.path.expanduser("~")
    path = os.path.join(base, "RawToJpegPro")
    os.makedirs(path, exist_ok=True)
    return path

SETTINGS_PATH = os.path.join(app_data_dir(), "settings.json")
LOGS_DIR = os.path.join(app_data_dir(), "logs")


def load_settings() -> dict:
    defaults = {
        "last_folder": "",
        "quality": 92,
        "overwrite": False,
        "include_subfolders": True,
        "workers": max(1, (os.cpu_count() or 4) - 1),
        "appearance": "System",  # "Light" / "Dark" / "System"
    }
    try:
        if os.path.exists(SETTINGS_PATH):
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                user = json.load(f)
            defaults.update({k: user.get(k, defaults[k]) for k in defaults})
    except Exception:
        pass
    return defaults


def save_settings(s: dict) -> None:
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception:
        pass


# -----------------------------
# RAW → JPEG conversion helpers
# -----------------------------

def safe_jpeg_path(raw_path: str) -> str:
    base, _ = os.path.splitext(raw_path)
    return base + ".jpg"


def extract_exif_from_raw(raw_path: str):
    """Return a piexif dict if possible, else None."""
    try:
        with rawpy.imread(raw_path) as raw:
            exif_bytes = getattr(raw.metadata, "exif", None)
        if not exif_bytes:
            return None
        return piexif.load(exif_bytes)
    except Exception:
        return None


def write_exif_to_jpeg(exif_dict, jpeg_path: str):
    """Insert EXIF into an existing JPEG. Safe no-op on errors."""
    if not exif_dict:
        return
    try:
        piexif.insert(piexif.dump(exif_dict), jpeg_path)
    except Exception:
        return


def minimal_exif_subset(exif_dict):
    """Safer EXIF subset for viewers."""
    if not exif_dict:
        return None

    out = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}

    for tag in (
        piexif.ImageIFD.Make,
        piexif.ImageIFD.Model,
        piexif.ImageIFD.Orientation,
        piexif.ImageIFD.Software,
        piexif.ImageIFD.DateTime,
    ):
        if tag in exif_dict.get("0th", {}):
            out["0th"][tag] = exif_dict["0th"][tag]

    for tag in (
        piexif.ExifIFD.DateTimeOriginal,
        piexif.ExifIFD.DateTimeDigitized,
        piexif.ExifIFD.ExposureTime,
        piexif.ExifIFD.FNumber,
        piexif.ExifIFD.ISOSpeedRatings,
        piexif.ExifIFD.FocalLength,
        piexif.ExifIFD.LensModel,
    ):
        if tag in exif_dict.get("Exif", {}):
            out["Exif"][tag] = exif_dict["Exif"][tag]

    return out


@dataclass(frozen=True)
class ConvertJob:
    raw_path: str
    quality: int
    overwrite: bool


def convert_one(job: ConvertJob):
    raw_path = job.raw_path
    jpg_path = safe_jpeg_path(raw_path)

    if (not job.overwrite) and os.path.exists(jpg_path):
        return ("skipped_exists", raw_path, jpg_path, None)

    try:
        exif = extract_exif_from_raw(raw_path)

        with rawpy.imread(raw_path) as raw:
            rgb = raw.postprocess(
                use_camera_wb=True,
                no_auto_bright=True,
                output_bps=8
            )

        imageio.imwrite(jpg_path, rgb, quality=int(job.quality))

        # EXIF: best effort
        if exif:
            write_exif_to_jpeg(exif, jpg_path)
            # If that didn't work for some reason, try safe subset
            # (piexif.insert can fail silently; we just attempt subset as second pass)
            write_exif_to_jpeg(minimal_exif_subset(exif), jpg_path)

        return ("ok", raw_path, jpg_path, None)
    except Exception as e:
        return ("error", raw_path, jpg_path, str(e))


def list_raws(folder: str, include_subfolders: bool):
    out = []
    if include_subfolders:
        for root, _, files in os.walk(folder):
            for fn in files:
                ext = os.path.splitext(fn)[1].lower()
                if ext in RAW_EXTS:
                    out.append(os.path.join(root, fn))
    else:
        for fn in os.listdir(folder):
            p = os.path.join(folder, fn)
            if os.path.isfile(p) and os.path.splitext(fn)[1].lower() in RAW_EXTS:
                out.append(p)
    return out


# -----------------------------
# UI
# -----------------------------

class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.settings = load_settings()

        ctk.set_appearance_mode(self.settings.get("appearance", "System"))
        ctk.set_default_color_theme("blue")

        self.title(f"{APP_NAME}  •  v{APP_VERSION}")
        self.geometry("980x600")
        self.minsize(980, 600)

        self.cancel_event = threading.Event()
        self.worker_thread = None

        # State vars
        self.folder_var = ctk.StringVar(value=self.settings.get("last_folder", ""))
        self.overwrite_var = ctk.BooleanVar(value=bool(self.settings.get("overwrite", False)))
        self.subfolders_var = ctk.BooleanVar(value=bool(self.settings.get("include_subfolders", True)))
        self.quality_var = ctk.IntVar(value=int(self.settings.get("quality", 92)))
        self.workers_var = ctk.IntVar(value=int(self.settings.get("workers", max(1, (os.cpu_count() or 4) - 1))))

        # Metrics
        self.total_files = 0
        self.done_files = 0
        self.ok = 0
        self.skipped = 0
        self.errors = 0
        self.start_time = None

        self._build_layout()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_layout(self):
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=1)

        # Sidebar
        sidebar = ctk.CTkFrame(self, corner_radius=18)
        sidebar.grid(row=0, column=0, sticky="nsw", padx=16, pady=16)

        ctk.CTkLabel(sidebar, text="RAW → JPEG", font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=0, column=0, padx=16, pady=(16, 4), sticky="w"
        )
        ctk.CTkLabel(sidebar, text="Professional batch converter", font=ctk.CTkFont(size=13)).grid(
            row=1, column=0, padx=16, pady=(0, 12), sticky="w"
        )

        ctk.CTkLabel(sidebar, text="Folder", font=ctk.CTkFont(size=13, weight="bold")).grid(
            row=2, column=0, padx=16, pady=(8, 6), sticky="w"
        )

        self.folder_entry = ctk.CTkEntry(sidebar, textvariable=self.folder_var, width=300)
        self.folder_entry.grid(row=3, column=0, padx=16, pady=(0, 10), sticky="w")

        ctk.CTkButton(sidebar, text="Browse…", command=self.pick_folder, height=34).grid(
            row=4, column=0, padx=16, pady=(0, 14), sticky="w"
        )

        ctk.CTkLabel(sidebar, text="Options", font=ctk.CTkFont(size=13, weight="bold")).grid(
            row=5, column=0, padx=16, pady=(0, 6), sticky="w"
        )

        ctk.CTkCheckBox(sidebar, text="Include subfolders", variable=self.subfolders_var).grid(
            row=6, column=0, padx=16, pady=(0, 8), sticky="w"
        )
        ctk.CTkCheckBox(sidebar, text="Overwrite existing JPGs", variable=self.overwrite_var).grid(
            row=7, column=0, padx=16, pady=(0, 12), sticky="w"
        )

        ctk.CTkLabel(sidebar, text="JPEG quality", font=ctk.CTkFont(size=12)).grid(
            row=8, column=0, padx=16, pady=(0, 0), sticky="w"
        )
        self.quality_slider = ctk.CTkSlider(
            sidebar, from_=50, to=100, number_of_steps=50,
            variable=self.quality_var, command=self._on_quality_change
        )
        self.quality_slider.grid(row=9, column=0, padx=16, pady=(6, 0), sticky="we")

        self.quality_label = ctk.CTkLabel(sidebar, text=f"{self.quality_var.get()}", font=ctk.CTkFont(size=12))
        self.quality_label.grid(row=10, column=0, padx=16, pady=(6, 12), sticky="w")

        ctk.CTkLabel(sidebar, text="Speed", font=ctk.CTkFont(size=12)).grid(
            row=11, column=0, padx=16, pady=(0, 0), sticky="w"
        )
        self.workers_slider = ctk.CTkSlider(
            sidebar, from_=1, to=max(2, os.cpu_count() or 8), number_of_steps=max(1, (os.cpu_count() or 8) - 1),
            variable=self.workers_var, command=self._on_workers_change
        )
        self.workers_slider.grid(row=12, column=0, padx=16, pady=(6, 0), sticky="we")

        self.workers_label = ctk.CTkLabel(sidebar, text=f"Workers: {self.workers_var.get()}", font=ctk.CTkFont(size=12))
        self.workers_label.grid(row=13, column=0, padx=16, pady=(6, 16), sticky="w")

        # Main area
        main = ctk.CTkFrame(self, corner_radius=18)
        main.grid(row=0, column=1, sticky="nsew", padx=(0, 16), pady=16)
        main.grid_columnconfigure(0, weight=1)
        main.grid_rowconfigure(3, weight=1)

        header = ctk.CTkFrame(main, fg_color="transparent")
        header.grid(row=0, column=0, sticky="ew", padx=16, pady=(16, 8))
        header.grid_columnconfigure(3, weight=1)

        self.convert_btn = ctk.CTkButton(header, text="Convert", command=self.start_convert, height=36)
        self.convert_btn.grid(row=0, column=0, sticky="w")

        self.cancel_btn = ctk.CTkButton(header, text="Cancel", command=self.cancel, height=36, state="disabled")
        self.cancel_btn.grid(row=0, column=1, padx=(10, 0), sticky="w")

        self.open_btn = ctk.CTkButton(header, text="Open folder", command=self.open_folder, height=36)
        self.open_btn.grid(row=0, column=2, padx=(10, 0), sticky="w")

        self.status_label = ctk.CTkLabel(header, text="Ready", anchor="w")
        self.status_label.grid(row=0, column=3, padx=(14, 0), sticky="ew")

        self.progress = ctk.CTkProgressBar(main)
        self.progress.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 6))
        self.progress.set(0)

        self.stats_label = ctk.CTkLabel(main, text="Converted: 0   Skipped: 0   Errors: 0", anchor="w")
        self.stats_label.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 10))

        self.log = ctk.CTkTextbox(main, corner_radius=12)
        self.log.grid(row=3, column=0, sticky="nsew", padx=16, pady=(0, 10))
        self.log.configure(state="normal")

        footer = ctk.CTkFrame(main, fg_color="transparent")
        footer.grid(row=4, column=0, sticky="ew", padx=16, pady=(0, 16))
        footer.grid_columnconfigure(1, weight=1)

        ctk.CTkButton(footer, text="Export log…", command=self.export_log, height=34).grid(row=0, column=0, sticky="w")
        self.time_label = ctk.CTkLabel(footer, text="", anchor="e")
        self.time_label.grid(row=0, column=1, sticky="e")

    def _on_quality_change(self, _value=None):
        self.quality_label.configure(text=str(int(self.quality_var.get())))

    def _on_workers_change(self, _value=None):
        self.workers_label.configure(text=f"Workers: {int(self.workers_var.get())}")

    def pick_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.folder_var.set(folder)

    def open_folder(self):
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showinfo("Open folder", "Choose a valid folder first.")
            return
        try:
            if sys.platform.startswith("win"):
                os.startfile(folder)  # type: ignore
            elif sys.platform == "darwin":
                os.system(f'open "{folder}"')
            else:
                os.system(f'xdg-open "{folder}"')
        except Exception:
            messagebox.showerror("Open folder", "Could not open the folder.")

    def start_convert(self):
        folder = self.folder_var.get().strip()
        if not folder or not os.path.isdir(folder):
            messagebox.showerror("Pick a folder", "Please choose a valid folder.")
            return

        # Reset counters
        self.cancel_event.clear()
        self.total_files = 0
        self.done_files = 0
        self.ok = self.skipped = self.errors = 0
        self.start_time = time.time()

        self.convert_btn.configure(state="disabled")
        self.cancel_btn.configure(state="normal")
        self.progress.set(0)
        self.log.delete("1.0", "end")
        self._set_status("Scanning files…")
        self._update_stats()

        t = threading.Thread(target=self.convert_worker, args=(folder,), daemon=True)
        self.worker_thread = t
        t.start()

    def cancel(self):
        self.cancel_event.set()
        self._log("Cancel requested…")

    def convert_worker(self, folder: str):
        include_subfolders = bool(self.subfolders_var.get())
        raws = list_raws(folder, include_subfolders)

        self.total_files = len(raws)
        self._ui(lambda: self._set_status(f"Found {self.total_files} RAW file(s)"))

        if self.total_files == 0:
            self._ui(self._finish_ui)
            return

        quality = int(self.quality_var.get())
        overwrite = bool(self.overwrite_var.get())
        workers = max(1, int(self.workers_var.get()))

        self._log(f"Starting conversion with {workers} worker(s)…")
        self._log(f"Folder: {folder}")
        self._log("")

        # Submit jobs in parallel; update UI as they complete.
        jobs = [ConvertJob(r, quality, overwrite) for r in raws]

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(convert_one, j): j.raw_path for j in jobs}

            for fut in as_completed(futures):
                if self.cancel_event.is_set():
                    self._log("")
                    self._log("Canceled. Waiting for running tasks to finish…")
                    break

                status, raw_path, jpg_path, err = fut.result()
                self.done_files += 1

                if status == "ok":
                    self.ok += 1
                    self._log(f"OK: {raw_path}")
                elif status == "skipped_exists":
                    self.skipped += 1
                    self._log(f"SKIP (exists): {raw_path}")
                else:
                    self.errors += 1
                    self._log(f"ERROR: {raw_path}")
                    if err:
                        self._log(f"      {err}")

                self._ui(self._tick_ui(raw_path))

        self._ui(self._finish_ui)

    def _tick_ui(self, raw_path: str):
        # return a callable so we can schedule it
        def fn():
            self.progress.set(self.done_files / max(1, self.total_files))
            self._set_status(os.path.basename(raw_path))
            self._update_stats()
            self._update_time()
        return fn

    def _finish_ui(self):
        self.progress.set(self.done_files / max(1, self.total_files))
        self._update_stats()
        self._update_time(final=True)

        if self.cancel_event.is_set():
            self._set_status("Canceled")
        else:
            self._set_status("Done")

        self.convert_btn.configure(state="normal")
        self.cancel_btn.configure(state="disabled")

        self._log("")
        self._log(f"Done. Converted: {self.ok}, Skipped: {self.skipped}, Errors: {self.errors}")

    def _update_stats(self):
        self.stats_label.configure(text=f"Converted: {self.ok}   Skipped: {self.skipped}   Errors: {self.errors}")

    def _update_time(self, final: bool = False):
        if not self.start_time:
            self.time_label.configure(text="")
            return
        elapsed = time.time() - self.start_time
        if final:
            self.time_label.configure(text=f"Time: {elapsed:.1f}s")
        else:
            self.time_label.configure(text=f"Elapsed: {elapsed:.1f}s")

    def export_log(self):
        os.makedirs(LOGS_DIR, exist_ok=True)
        default_name = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            initialdir=LOGS_DIR,
            initialfile=default_name,
            filetypes=[("Text files", "*.txt")]
        )
        if not path:
            return
        try:
            text = self.log.get("1.0", "end")
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            messagebox.showinfo("Export log", "Log saved.")
        except Exception:
            messagebox.showerror("Export log", "Could not save the log.")

    def _set_status(self, text: str):
        self.status_label.configure(text=text)

    def _log(self, msg: str):
        self._ui(lambda: (self.log.insert("end", msg + "\n"), self.log.see("end")))

    def _ui(self, fn):
        self.after(0, fn)

    def on_close(self):
        # Save settings
        self.settings["last_folder"] = self.folder_var.get()
        self.settings["quality"] = int(self.quality_var.get())
        self.settings["overwrite"] = bool(self.overwrite_var.get())
        self.settings["include_subfolders"] = bool(self.subfolders_var.get())
        self.settings["workers"] = int(self.workers_var.get())
        save_settings(self.settings)

        # Try to cancel running work politely
        self.cancel_event.set()
        self.destroy()


if __name__ == "__main__":
    App().mainloop()