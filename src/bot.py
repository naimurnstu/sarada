"""
bot.py — Application entrypoint.

Responsibilities:
  • Bootstrap logging
  • Load config
  • Wire dependencies
  • Register handlers
  • Start polling with graceful shutdown
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
)

import auth
import config as cfg_module
from config import Config
from handlers import BotHandlers
from storage import CookieStore, ProfileStore


# ── Logging setup ──────────────────────────────────────────────────────────────

def _setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Console handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    # Rotating file handler — 5 MB × 3 backups
    fh = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Silence noisy third-party loggers
    for noisy in ("httpx", "httpcore", "urllib3", "telegram.vendor"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Bootstrap ──────────────────────────────────────────────────────────────────

def _ensure_dirs(cfg: Config) -> None:
    """Create all required directories on startup."""
    for path in (cfg.base_dir, cfg.cookies_dir, cfg.profiles_file.parent, cfg.log_file.parent):
        path.mkdir(parents=True, exist_ok=True)


def _build_app(cfg: Config) -> Application:
    """Wire all dependencies and return a ready-to-run Application."""
    profiles = ProfileStore(cfg)
    cookies  = CookieStore(cfg)
    h        = BotHandlers(cfg=cfg, profiles=profiles, cookies=cookies)

    app = Application.builder().token(cfg.bot_token).build()

    # ── Public commands ────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",   h.cmd_start))
    app.add_handler(CommandHandler("help",    h.cmd_start))

    # ── Owner-only commands ────────────────────────────────────────────────
    app.add_handler(CommandHandler("add",     h.cmd_add))
    app.add_handler(CommandHandler("remove",  h.cmd_remove))
    app.add_handler(CommandHandler("list",    h.cmd_list))
    app.add_handler(CommandHandler("clear",   h.cmd_clear))
    app.add_handler(CommandHandler("cookies", h.cmd_cookies))
    app.add_handler(CommandHandler("status",  h.cmd_status))
    app.add_handler(CommandHandler("cancel",  h.cmd_cancel))
    app.add_handler(CommandHandler("run",     h.cmd_run))

    # ── Document uploads (cookies + bulk import) ───────────────────────────
    app.add_handler(MessageHandler(filters.Document.ALL, h.handle_document))

    return app


def main() -> None:
    # 1. Load config (raises if BOT_TOKEN missing)
    try:
        cfg = cfg_module.load()
    except RuntimeError as exc:
        # Can't log yet — print directly and exit
        print(f"[FATAL] Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    # 2. Setup logging
    _setup_logging(cfg.log_file)
    logger.info("=" * 60)
    logger.info("Starting Social Media Downloader Bot")
    logger.info("Owner ID : %s", cfg.owner_id or "NOT SET — open access!")
    logger.info("Base dir : %s", cfg.base_dir)
    logger.info("Platforms: %s", ", ".join(cfg.platforms))
    logger.info("=" * 60)

    # 3. Warn if owner not configured
    if cfg.owner_id == 0:
        logger.critical(
            "OWNER_ID environment variable is not set. "
            "Anyone can control this bot. Set OWNER_ID immediately."
        )

    # 4. Configure auth module
    auth.configure(cfg.owner_id)

    # 5. Ensure directory layout exists
    _ensure_dirs(cfg)

    # 6. Build and start application
    app = _build_app(cfg)

    logger.info("Bot is polling for updates…")
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=["message"],
    )


if __name__ == "__main__":
    main()
