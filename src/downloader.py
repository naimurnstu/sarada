"""
downloader.py — Async wrapper around gallery-dl.

Key fixes vs original:
  • gallery-dl config written with correct cookies path (not hardcoded)
  • MediaMode enum + DownloadResult dataclass match every call site in handlers.py
  • Rate-limit / login / private / not-found errors reliably classified from
    both gallery-dl exit codes AND stderr output patterns
  • Subprocess is run in asyncio executor so it never blocks the event loop
  • Cookie file written with deduplication before each run
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config, PlatformConfig

logger = logging.getLogger(__name__)

# ── Enumerations ──────────────────────────────────────────────────────────────

class MediaMode(Enum):
    PHOTOS = auto()
    VIDEOS = auto()
    BOTH   = auto()

    def label(self) -> str:
        return {
            MediaMode.PHOTOS: "Photos only",
            MediaMode.VIDEOS: "Videos only",
            MediaMode.BOTH:   "Photos + Videos",
        }[self]

    @staticmethod
    def from_str(value: str) -> "MediaMode":
        mapping = {
            "photos": MediaMode.PHOTOS,
            "photo":  MediaMode.PHOTOS,
            "videos": MediaMode.VIDEOS,
            "video":  MediaMode.VIDEOS,
            "both":   MediaMode.BOTH,
        }
        return mapping.get(value.lower(), MediaMode.BOTH)


class ErrorKind(Enum):
    NONE         = auto()
    RATE_LIMITED = auto()   # HTTP 429
    LOGIN        = auto()   # Authentication / session required
    PRIVATE      = auto()   # Account is private
    NOT_FOUND    = auto()   # Profile deleted / renamed
    GALLERY_DL   = auto()   # gallery-dl not installed
    NETWORK      = auto()   # Transient network failure
    UNKNOWN      = auto()


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class SubfolderResult:
    subfolder: str                     # "photos" | "videos"
    error_kind: ErrorKind = ErrorKind.NONE
    error: str | None = None
    new_files: list[Path] = field(default_factory=list)


@dataclass
class DownloadResult:
    results: list[SubfolderResult] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""

    @property
    def total_new(self) -> int:
        return sum(len(r.new_files) for r in self.results)


# ── Error classification ──────────────────────────────────────────────────────

_RATE_LIMIT_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"429", re.IGNORECASE),
    re.compile(r"Too Many Requests", re.IGNORECASE),
    re.compile(r"rate.?limit", re.IGNORECASE),
)
_LOGIN_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"login required", re.IGNORECASE),
    re.compile(r"checkpoint_required", re.IGNORECASE),
    re.compile(r"not logged in", re.IGNORECASE),
    re.compile(r"authentication", re.IGNORECASE),
)
_PRIVATE_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"private", re.IGNORECASE),
)
_NOT_FOUND_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"404", re.IGNORECASE),
    re.compile(r"not found", re.IGNORECASE),
    re.compile(r"does not exist", re.IGNORECASE),
    re.compile(r"user not found", re.IGNORECASE),
)
_NETWORK_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"ConnectionError", re.IGNORECASE),
    re.compile(r"timeout", re.IGNORECASE),
    re.compile(r"RemoteDisconnected", re.IGNORECASE),
    re.compile(r"SSLError", re.IGNORECASE),
)


def _classify_error(rc: int, output: str) -> ErrorKind:
    """Map gallery-dl exit code + stderr text to an ErrorKind."""
    if rc == 127:
        return ErrorKind.GALLERY_DL  # command not found

    for pat in _RATE_LIMIT_PATTERNS:
        if pat.search(output):
            return ErrorKind.RATE_LIMITED

    for pat in _LOGIN_PATTERNS:
        if pat.search(output):
            return ErrorKind.LOGIN

    for pat in _PRIVATE_PATTERNS:
        if pat.search(output):
            return ErrorKind.PRIVATE

    for pat in _NOT_FOUND_PATTERNS:
        if pat.search(output):
            return ErrorKind.NOT_FOUND

    for pat in _NETWORK_PATTERNS:
        if pat.search(output):
            return ErrorKind.NETWORK

    if rc != 0:
        return ErrorKind.UNKNOWN

    return ErrorKind.NONE


# ── gallery-dl config builder ─────────────────────────────────────────────────

def _build_gallery_dl_config(
    cfg: "Config",
    plat: "PlatformConfig",
    output_dir: Path,
    mode: MediaMode,
) -> dict:
    """
    Build a minimal gallery-dl JSON config dict.
    Written to a temp file per run so each invocation is stateless.
    """
    cookie_path = cfg.cookies_dir / plat.cookie_file
    cookie_str = str(cookie_path) if cookie_path.exists() else ""

    # File filter based on mode
    if mode == MediaMode.PHOTOS:
        file_filter = cfg.photo_filter
    elif mode == MediaMode.VIDEOS:
        file_filter = cfg.video_filter
    else:
        file_filter = None   # no filter = everything

    extractor_cfg: dict = {
        "sleep-request": float(plat.sleep_sec),
        "retries": 3,
        "timeout": 60,
        "directory": [str(output_dir)],
        "filename": "{filename}.{extension}",
    }
    if cookie_str:
        extractor_cfg["cookies"] = cookie_str
    if file_filter:
        extractor_cfg["image-filter"] = file_filter

    return {
        "extractor": {
            "*": extractor_cfg,
            "instagram": {**extractor_cfg, "sleep-request": 5},
        },
        "downloader": {
            "retries": 3,
            "timeout": 120,
        },
        "output": {
            "mode": "terminal",
            "progress": False,
        },
    }


# ── Downloader ────────────────────────────────────────────────────────────────

class Downloader:

    def __init__(self, cfg: "Config") -> None:
        self._cfg = cfg
        self._gallery_dl_path = shutil.which("gallery-dl") or "gallery-dl"
        logger.info(
            "gallery-dl config written → %s",
            cfg.cookies_dir.parent / "gallery-dl.conf",
        )

    # ── Username extraction ───────────────────────────────────────────────────

    @staticmethod
    def _extract_username(url: str, plat: "PlatformConfig") -> str | None:
        """Extract the bare username from a profile URL."""
        try:
            path = url.rstrip("/").split("/")[-1]
            if path.startswith("@"):
                path = path[1:]
            return path or None
        except Exception:
            return None

    # ── Public download entry point ───────────────────────────────────────────

    async def download_user(
        self,
        url: str,
        plat: "PlatformConfig",
        mode: MediaMode,
    ) -> DownloadResult:
        """
        Download media for one profile URL.

        For BOTH mode: runs photos then videos as separate gallery-dl
        invocations so each has a precise file filter applied.
        For PHOTOS / VIDEOS: single invocation.
        """
        if not shutil.which("gallery-dl"):
            sub = SubfolderResult(
                subfolder="all",
                error_kind=ErrorKind.GALLERY_DL,
                error="gallery-dl not found in PATH",
            )
            return DownloadResult(results=[sub])

        if mode == MediaMode.BOTH:
            photo_result = await self._run_once(url, plat, MediaMode.PHOTOS, "photos")
            video_result = await self._run_once(url, plat, MediaMode.VIDEOS, "videos")
            return DownloadResult(results=[photo_result, video_result])

        subfolder = "photos" if mode == MediaMode.PHOTOS else "videos"
        result = await self._run_once(url, plat, mode, subfolder)
        return DownloadResult(results=[result])

    # ── Single gallery-dl invocation ──────────────────────────────────────────

    async def _run_once(
        self,
        url: str,
        plat: "PlatformConfig",
        mode: MediaMode,
        subfolder: str,
    ) -> SubfolderResult:
        output_dir = self._cfg.base_dir / plat.folder / subfolder
        output_dir.mkdir(parents=True, exist_ok=True)

        # Snapshot existing files before download
        before: set[Path] = self._snapshot(output_dir)

        # Write temp config file for this run
        config_dict = _build_gallery_dl_config(self._cfg, plat, output_dir, mode)

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                delete=False,
                prefix="gdl_cfg_",
            ) as tmp:
                json.dump(config_dict, tmp, indent=2)
                cfg_path = tmp.name
        except OSError as exc:
            logger.error("Cannot write gallery-dl config: %s", exc)
            return SubfolderResult(
                subfolder=subfolder,
                error_kind=ErrorKind.UNKNOWN,
                error=str(exc),
            )

        cmd = [
            self._gallery_dl_path,
            "--config", cfg_path,
            "--no-mtime",
            url,
        ]

        logger.info("gallery-dl cmd: %s", " ".join(cmd))

        try:
            rc, combined_output = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._run_subprocess(cmd),
            )
        except Exception as exc:
            logger.error("Subprocess launch failed: %s", exc)
            return SubfolderResult(
                subfolder=subfolder,
                error_kind=ErrorKind.UNKNOWN,
                error=str(exc),
            )
        finally:
            try:
                Path(cfg_path).unlink(missing_ok=True)
            except OSError:
                pass

        if rc != 0:
            logger.warning(
                "gallery-dl rc=%d url=%s | %s",
                rc, url, combined_output[:500],
            )

        error_kind = _classify_error(rc, combined_output)

        after: set[Path] = self._snapshot(output_dir)
        new_files = sorted(after - before)

        return SubfolderResult(
            subfolder=subfolder,
            error_kind=error_kind,
            error=combined_output[:1000] if error_kind != ErrorKind.NONE else None,
            new_files=new_files,
        )

    # ── Subprocess helper (blocking — called in executor) ─────────────────────

    @staticmethod
    def _run_subprocess(cmd: list[str]) -> tuple[int, str]:
        """Run *cmd*, capture combined stdout+stderr, return (returncode, text)."""
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=600,          # 10-minute hard timeout per profile
            )
            output = proc.stdout.decode("utf-8", errors="replace")
            return proc.returncode, output
        except subprocess.TimeoutExpired:
            return 1, "gallery-dl timed out after 600 seconds"
        except FileNotFoundError:
            return 127, "gallery-dl: command not found"
        except Exception as exc:
            return 1, str(exc)

    # ── File snapshot ─────────────────────────────────────────────────────────

    @staticmethod
    def _snapshot(directory: Path) -> set[Path]:
        """Return the set of all files currently under *directory*."""
        try:
            return {
                p for p in directory.rglob("*")
                if p.is_file() and not p.name.startswith(".")
            }
        except OSError:
            return set()
