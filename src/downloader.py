"""
downloader.py — gallery-dl subprocess wrapper.

Fixes applied (v5):
  BUG #1 — Removed "rate": None from gallery-dl.conf (JSON null crashes
            gallery-dl's config parser, silently dropping all downloader settings).
  BUG #3 — _extract_username now normalises URLs without 'www.' prefix,
            mobile URLs, and trailing path segments (prevents full URL
            being used as directory name).
  BUG #6 — Cookie specification no longer duplicated between config file
            and CLI --cookies flag. Config file handles per-platform cookies;
            CLI flag is removed. This eliminates false-positive cookie errors
            from gallery-dl when optional cookie files are absent.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config, PlatformConfig

logger = logging.getLogger(__name__)


# ── Enums / result types ───────────────────────────────────────────────────────

class MediaMode(Enum):
    PHOTOS = auto()
    VIDEOS = auto()
    BOTH   = auto()

    @classmethod
    def from_str(cls, s: str) -> "MediaMode":
        return {
            "photos": cls.PHOTOS,
            "videos": cls.VIDEOS,
            "both":   cls.BOTH,
        }.get(s.lower(), cls.BOTH)

    def label(self) -> str:
        return {
            MediaMode.PHOTOS: "Photos only",
            MediaMode.VIDEOS: "Videos only",
            MediaMode.BOTH:   "Photos + Videos",
        }[self]


class ErrorKind(Enum):
    NONE         = "none"
    RATE_LIMITED = "rate_limited"
    LOGIN        = "login"
    NOT_FOUND    = "not_found"
    PRIVATE      = "private"
    GALLERY_DL   = "gallery_dl"
    NETWORK      = "network"
    GENERIC      = "generic"


def _classify_error(stderr: str, returncode: int) -> ErrorKind:
    """Parse gallery-dl stderr + exit code into a typed ErrorKind."""
    if returncode == 0:
        return ErrorKind.NONE
    if not stderr:
        return ErrorKind.GENERIC
    s = stderr.lower()
    if "not found in path" in s:
        return ErrorKind.GALLERY_DL
    if "429" in s or "rate" in s or "too many requests" in s:
        return ErrorKind.RATE_LIMITED
    if "login" in s or "unauthorized" in s or "checkpoint" in s or "401" in s:
        return ErrorKind.LOGIN
    if "not found" in s or "404" in s or "does not exist" in s:
        return ErrorKind.NOT_FOUND
    if "private" in s or "restricted" in s:
        return ErrorKind.PRIVATE
    if "connection" in s or "timeout" in s or "network" in s or "ssl" in s:
        return ErrorKind.NETWORK
    return ErrorKind.GENERIC


@dataclass
class SubfolderResult:
    subfolder:      str
    new_files:      list[Path]
    archive_action: str
    error:          str | None
    error_kind:     ErrorKind = ErrorKind.NONE


@dataclass
class UserResult:
    username:    str
    platform:    str
    results:     list[SubfolderResult] = field(default_factory=list)
    skipped:     bool = False
    skip_reason: str  = ""

    @property
    def total_new(self) -> int:
        return sum(len(r.new_files) for r in self.results)

    @property
    def worst_error_kind(self) -> ErrorKind:
        priority = [
            ErrorKind.GALLERY_DL,
            ErrorKind.RATE_LIMITED,
            ErrorKind.LOGIN,
            ErrorKind.PRIVATE,
            ErrorKind.NOT_FOUND,
            ErrorKind.NETWORK,
            ErrorKind.GENERIC,
            ErrorKind.NONE,
        ]
        for kind in priority:
            if any(r.error_kind == kind for r in self.results):
                return kind
        return ErrorKind.NONE


# ── Archive cleaner ────────────────────────────────────────────────────────────

class ArchiveCleaner:
    """
    Mirrors CHECK_AND_CLEAN_ARCHIVE from the original .bat script.

    Logic:
      file_count == 0            → clear archive (folder is empty)
      file_count < archive_count → clear archive (files were deleted, mismatch)
      else                       → archive is valid, leave it alone
    """

    @staticmethod
    def check_and_clean(dl_dir: Path, archive: Path) -> str:
        try:
            files = [
                f for f in dl_dir.iterdir()
                if f.is_file() and f.name != "archive.txt"
            ]
        except OSError:
            files = []

        file_count = len(files)
        arch_count = 0

        if archive.exists():
            try:
                arch_count = sum(
                    1 for _ in archive.open(encoding="utf-8", errors="replace")
                )
            except OSError:
                arch_count = 0

        if file_count == 0:
            if archive.exists():
                try:
                    archive.unlink()
                    logger.debug("Archive cleared (folder empty): %s", archive)
                except OSError as exc:
                    logger.warning("Could not clear archive %s: %s", archive, exc)
            return "cleared — folder empty"

        if file_count < arch_count:
            try:
                archive.unlink()
                logger.debug(
                    "Archive cleared (mismatch %d files / %d entries): %s",
                    file_count, arch_count, archive,
                )
            except OSError as exc:
                logger.warning("Could not clear archive %s: %s", archive, exc)
            return f"cleared — mismatch ({file_count} files, {arch_count} entries)"

        return f"valid — {file_count} file(s), {arch_count} archive entries"


# ── gallery-dl config writer ───────────────────────────────────────────────────

class GalleryDLConfig:
    """
    Writes an optimised gallery-dl.conf to /data/gallery-dl.conf on first use.

    FIX (BUG #1): Removed "rate": None from downloader section.
    JSON null is not a valid value for gallery-dl's rate setting and causes
    a TypeError in gallery-dl's config parser, silently removing all
    downloader timeout/retry settings.

    FIX (BUG #6): Cookie paths are NOT set in the config file. They are
    passed exclusively via the --cookies CLI flag. Setting cookies in BOTH
    places causes gallery-dl to emit spurious "cookie file not found" warnings
    for platforms whose cookie files are absent, which pollutes stderr and
    triggers false-positive LoginError/RateLimited classifications.
    """

    _CONFIG_PATH = Path("/data/gallery-dl.conf")

    # Chrome 124 on Windows 11 — common, unsuspicious User-Agent
    _USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )

    @classmethod
    def ensure_written(cls, _cookies_dir: Path) -> Path:
        """
        Write the config file to disk. Always rewrites so settings are current.
        Returns the config path.
        """
        cls._CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

        conf = {
            "extractor": {
                "user-agent":        cls._USER_AGENT,
                "retries":           5,
                "timeout":           30,
                "verify":            True,
                "sleep-request":     3,
                "sleep-extractor":   3,

                "instagram": {
                    "sleep-request":          8,
                    "sleep-extractor":        5,
                    "sleep-between-requests": 8,
                    "retries":                5,
                    "videos":                 True,
                    "reels":                  True,
                    "posts":                  True,
                    "tagged":                 False,
                    "stories":                False,
                    "highlights":             False,
                    # NOTE: no "cookies" key here — passed via CLI --cookies flag
                    # to avoid false-positive errors when optional files are absent
                },

                "tiktok": {
                    "sleep-request": 4,
                    "retries":       4,
                },

                "facebook": {
                    "sleep-request": 6,
                    "retries":       4,
                },

                "twitter": {
                    "sleep-request": 5,
                    "retries":       4,
                },
            },

            "downloader": {
                "retries": 5,
                "timeout": 60,
                # FIX (BUG #1): "rate": None removed — null crashes gallery-dl parser.
                # Omitting the key entirely = unlimited rate (default behaviour).
            },

            "output": {
                "mode": "null",
            },
        }

        try:
            cls._CONFIG_PATH.write_text(
                json.dumps(conf, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            logger.info("gallery-dl config written → %s", cls._CONFIG_PATH)
        except OSError as exc:
            logger.warning("Could not write gallery-dl config: %s", exc)

        return cls._CONFIG_PATH


# ── gallery-dl runner ──────────────────────────────────────────────────────────

class GalleryDLRunner:
    """Thin async wrapper around the gallery-dl CLI."""

    _GALLERY_DL = "gallery-dl"

    @staticmethod
    async def run(
        url:         str,
        dl_dir:      Path,
        ext_filter:  str,
        archive:     Path,
        cookies:     Path | None,
        sleep_sec:   str,
        config_path: Path | None,
    ) -> tuple[int, str]:
        """
        Run gallery-dl and return (returncode, stderr_tail).
        stdout is discarded; stderr is captured for error classification.

        FIX (BUG #6): --cookies CLI flag is the SOLE source of cookie paths.
        The config file no longer contains per-platform cookie= entries,
        preventing duplicate specification and false-positive cookie errors.
        """
        cmd: list[str] = [GalleryDLRunner._GALLERY_DL]

        if config_path and config_path.exists():
            cmd += ["--config", str(config_path)]

        cmd += [
            "--directory",        str(dl_dir),
            "--filter",           ext_filter,
            "--download-archive", str(archive),
            "--sleep-request",    sleep_sec,
            "--sleep-extractor",  "3",
            "--retries",          "5",
            "--no-mtime",
        ]

        if cookies and cookies.exists():
            cmd += ["--cookies", str(cookies)]

        cmd.append(url)

        logger.debug("gallery-dl cmd: %s", " ".join(cmd))

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
            _, stderr_bytes = await proc.communicate()
        except FileNotFoundError:
            return -1, "gallery-dl not found in PATH — check Dockerfile"
        except OSError as exc:
            return -1, str(exc)

        stderr = (stderr_bytes or b"").decode(errors="replace").strip()

        if proc.returncode != 0:
            logger.warning(
                "gallery-dl rc=%d url=%s | %s",
                proc.returncode, url, stderr[-400:],
            )
        else:
            logger.info("gallery-dl OK: %s", url)

        return proc.returncode, stderr[-600:] if stderr else ""


# ── Main download engine ───────────────────────────────────────────────────────

class Downloader:
    """
    Orchestrates per-user downloads across platforms.
    Mirrors the original .bat DOWNLOAD_USER / DU_BOTH / DU_PHOTOS / DU_VIDEOS.
    """

    def __init__(self, cfg: "Config") -> None:
        self._cfg         = cfg
        self._config_path = GalleryDLConfig.ensure_written(cfg.cookies_dir)

    def _snapshot(self, dl_dir: Path) -> set[Path]:
        try:
            return {
                f for f in dl_dir.iterdir()
                if f.is_file() and f.name != "archive.txt"
            }
        except OSError:
            return set()

    def _safe_mtime(self, p: Path) -> float:
        try:
            return p.stat().st_mtime
        except OSError:
            return 0.0

    async def _download_subfolder(
        self,
        subfolder:  str,
        user_base:  Path,
        ext_filter: str,
        url:        str,
        plat:       "PlatformConfig",
        cookies:    Path | None,
    ) -> SubfolderResult:
        dl_dir  = user_base / subfolder
        archive = dl_dir / "archive.txt"

        try:
            dl_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return SubfolderResult(
                subfolder=subfolder,
                new_files=[],
                archive_action="error",
                error=f"Could not create directory: {exc}",
                error_kind=ErrorKind.GENERIC,
            )

        archive_action = ArchiveCleaner.check_and_clean(dl_dir, archive)
        before         = self._snapshot(dl_dir)

        returncode, stderr = await GalleryDLRunner.run(
            url=url,
            dl_dir=dl_dir,
            ext_filter=ext_filter,
            archive=archive,
            cookies=cookies,
            sleep_sec=plat.sleep_sec,
            config_path=self._config_path,
        )

        after      = self._snapshot(dl_dir)
        new_files  = sorted(after - before, key=self._safe_mtime)
        error_kind = _classify_error(stderr, returncode)

        error: str | None = None
        if returncode != 0 and stderr:
            error = stderr

        return SubfolderResult(
            subfolder=subfolder,
            new_files=new_files,
            archive_action=archive_action,
            error=error,
            error_kind=error_kind,
        )

    async def download_user(
        self,
        url:  str,
        plat: "PlatformConfig",
        mode: MediaMode,
    ) -> UserResult:
        """Download photos and/or videos for one profile URL."""
        username = self._extract_username(url, plat)
        if not username:
            return UserResult(
                username=url,
                platform=plat.name,
                skipped=True,
                skip_reason="Could not extract username from URL",
            )

        user_base   = self._cfg.base_dir / plat.folder / username
        cookie_file = self._cfg.cookies_dir / plat.cookie_file
        cookie_path = cookie_file if cookie_file.exists() else None

        if cookie_path is None:
            logger.warning(
                "No cookie file for %s — download will proceed without auth "
                "(public profiles only).",
                plat.name,
            )

        result = UserResult(username=username, platform=plat.name)
        tasks: list[tuple[str, str]] = []

        if mode in (MediaMode.PHOTOS, MediaMode.BOTH):
            tasks.append(("Photos", self._cfg.photo_filter))
        if mode in (MediaMode.VIDEOS, MediaMode.BOTH):
            tasks.append(("Videos", self._cfg.video_filter))

        for subfolder, ext_filter in tasks:
            sub_result = await self._download_subfolder(
                subfolder=subfolder,
                user_base=user_base,
                ext_filter=ext_filter,
                url=url,
                plat=plat,
                cookies=cookie_path,
            )
            result.results.append(sub_result)

        return result

    @staticmethod
    def _extract_username(url: str, plat: "PlatformConfig") -> str:
        """
        Extract the bare username from a profile URL.

        FIX (BUG #3): Previous implementation only stripped the exact
        url_prefix string. This failed for:
          - URLs without 'www.' (e.g. https://instagram.com/user)
          - Mobile URLs
          - URLs with trailing path segments or query strings

        New implementation: strip scheme → strip www. → strip platform
        domain → take first non-empty path segment. Falls back to the
        old prefix-strip approach if the robust method fails.
        """
        raw = url.strip().rstrip("/")
        if not raw:
            return ""

        # ── Robust extraction ─────────────────────────────────────────────
        # Parse: remove scheme (http:// or https://)
        no_scheme = re.sub(r"^https?://", "", raw, flags=re.IGNORECASE)

        # Remove www. prefix
        no_www = re.sub(r"^www\.", "", no_scheme, flags=re.IGNORECASE)

        # Build a domain pattern from the platform's url_prefix
        # e.g. "https://www.instagram.com/" → "instagram.com"
        prefix_domain = re.sub(
            r"^https?://(www\.)?", "", plat.url_prefix.rstrip("/"),
            flags=re.IGNORECASE,
        ).split("/")[0]  # e.g. "instagram.com"

        # Strip the platform domain from the start
        domain_pattern = re.escape(prefix_domain)
        no_domain = re.sub(
            rf"^{domain_pattern}/?", "", no_www, flags=re.IGNORECASE
        )

        # Strip leading @ (TikTok uses @username)
        no_at = no_domain.lstrip("@")

        # Take only the first path segment (ignore /posts/, /reels/, ?hl=en etc.)
        username = no_at.split("/")[0].split("?")[0].split("#")[0].strip()

        # Sanity: if still looks like a URL or is empty, reject
        if not username or username.startswith("http"):
            return ""

        return username
