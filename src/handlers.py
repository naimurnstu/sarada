"""
handlers.py — All Telegram command, callback, and document handlers.

Access model:
  • Private chat    → owner only
  • Group (listed)  → anyone can: /run /list /status /cookies /start
                      owner only: /add /remove /clear /allowgroup /denygroup
  • Topics (forum)  → each username gets its own topic thread
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import ContextTypes

import auth
from downloader import Downloader, MediaMode

if TYPE_CHECKING:
    from config import Config
    from storage import CookieStore, GroupStore, ProfileStore, TopicStore

logger = logging.getLogger(__name__)

# ── Platform metadata ──────────────────────────────────────────────────────────

PLATFORM_EMOJI: dict[str, str] = {
    "instagram": "📸",
    "tiktok":    "🎵",
    "facebook":  "💙",
    "x":         "🐦",
}

# Valid icon_color values for create_forum_topic
TOPIC_COLORS: dict[str, int] = {
    "instagram": 0xFF93B2,   # rose
    "tiktok":    0xFB6F5F,   # red-orange
    "facebook":  0x6FB9F0,   # sky-blue
    "x":         0xCB86DB,   # purple
}

# ── Markup builders ────────────────────────────────────────────────────────────

def _main_menu(is_owner: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("▶️ Photos",  callback_data="run:photos"),
            InlineKeyboardButton("▶️ Videos",  callback_data="run:videos"),
            InlineKeyboardButton("📦 Both",    callback_data="run:both"),
        ],
        [
            InlineKeyboardButton("📋 Profiles", callback_data="menu:list"),
            InlineKeyboardButton("📊 Status",   callback_data="menu:status"),
            InlineKeyboardButton("🍪 Cookies",  callback_data="menu:cookies"),
        ],
        [
            InlineKeyboardButton("❌ Cancel Run", callback_data="menu:cancel"),
        ],
    ]
    if is_owner:
        rows.append([
            InlineKeyboardButton("✅ Allow This Group", callback_data="grp:allow_here"),
            InlineKeyboardButton("📋 My Groups",        callback_data="grp:list"),
        ])
        rows.append([
            InlineKeyboardButton("🗑 Remove a Group", callback_data="grp:remove_prompt"),
        ])
    return InlineKeyboardMarkup(rows)


def _back_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu:main"),
    ]])


# ── Text helpers ───────────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Escape MarkdownV2 special characters."""
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


async def _send(
    ctx:       ContextTypes.DEFAULT_TYPE,
    chat_id:   int,
    text:      str,
    thread_id: int | None = None,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    """Send a MarkdownV2 message, optionally to a forum topic thread."""
    try:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=text[:4096],
            parse_mode=ParseMode.MARKDOWN_V2,
            message_thread_id=thread_id,
            reply_markup=reply_markup,
        )
    except TelegramError as exc:
        logger.error("send failed chat=%d thread=%s: %s", chat_id, thread_id, exc)


# ── BotHandlers ────────────────────────────────────────────────────────────────

class BotHandlers:
    def __init__(
        self,
        cfg:      "Config",
        profiles: "ProfileStore",
        cookies:  "CookieStore",
        groups:   "GroupStore",
        topics:   "TopicStore",
    ) -> None:
        self._cfg      = cfg
        self._profiles = profiles
        self._cookies  = cookies
        self._groups   = groups
        self._topics   = topics
        self._dl       = Downloader(cfg)
        self._running  = False

    # ── Access guard ──────────────────────────────────────────────────────────

    def _ok(self, update: Update, require_owner: bool = False) -> bool:
        return auth.check(update, require_owner=require_owner)

    def _user_is_owner(self, update: Update) -> bool:
        u = update.effective_user
        return u is not None and auth.is_owner(u.id)

    # ── /start ────────────────────────────────────────────────────────────────

    async def cmd_start(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        chat = update.effective_chat
        user = update.effective_user

        # Groups: must be whitelisted (no require_owner check here — /start is public-ish)
        if chat.type != ChatType.PRIVATE and not auth.is_group_allowed(chat.id):
            return

        is_owner = self._user_is_owner(update)
        markup   = _main_menu(is_owner)

        text = (
            "🤖 *Social Media Downloader*\n\n"
            "📌 *How to use:*\n"
            "1\\. Send `instagram\\_profiles\\.txt` to import profiles\n"
            "2\\. Send `instagram\\.com\\_cookies\\.txt` to add cookies\n"
            "3\\. Press *▶️ Run* to start downloading\n\n"
            "📂 *Platforms:* Instagram · TikTok · Facebook · X\n\n"
            "⬇️ Choose an action below:"
        )
        await _send(ctx, chat.id, text, reply_markup=markup)

    # ── Callback router ───────────────────────────────────────────────────────

    async def handle_callback(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        await query.answer()

        chat = update.effective_chat
        data = query.data or ""

        # Group whitelist check (private is owner-only via _ok)
        if chat.type != ChatType.PRIVATE and not auth.is_group_allowed(chat.id):
            return

        if data.startswith("run:"):
            mode = MediaMode.from_str(data.split(":")[1])
            await self._execute_run(update, ctx, mode)

        elif data == "menu:list":
            await self._cb_list(update, ctx)

        elif data == "menu:status":
            await self._cb_status(update, ctx)

        elif data == "menu:cookies":
            await self._cb_cookies(update, ctx)

        elif data == "menu:cancel":
            await self._cb_cancel(update, ctx)

        elif data == "menu:main":
            await self.cmd_start(update, ctx)

        elif data == "grp:allow_here":
            if not self._user_is_owner(update):
                return
            await self._cb_allow_here(update, ctx)

        elif data == "grp:list":
            if not self._user_is_owner(update):
                return
            await self._cb_groups_list(update, ctx)

        elif data == "grp:remove_prompt":
            if not self._user_is_owner(update):
                return
            await self._cb_remove_group_prompt(update, ctx)

        elif data.startswith("grp:deny:"):
            if not self._user_is_owner(update):
                return
            gid = int(data.split(":")[2])
            self._groups.deny(gid)
            await _send(
                ctx, chat.id,
                f"🗑 Group `{_esc(str(gid))}` removed from whitelist\\.",
                reply_markup=_back_button(),
            )

    # ── Inline callback implementations ───────────────────────────────────────

    async def _cb_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat     = update.effective_chat
        profiles = self._profiles.all()
        lines: list[str] = []

        for platform, urls in profiles.items():
            if not urls:
                continue
            em   = PLATFORM_EMOJI.get(platform, "📁")
            plat = self._cfg.platforms[platform]
            lines.append(f"*{em} {_esc(plat.label)}* \\({len(urls)}\\)")
            for url in urls:
                uname = Downloader._extract_username(url, plat) or url
                lines.append(f"  • `{_esc(uname)}`")

        total = self._profiles.total_count()
        if lines:
            lines.append(f"\n_Total: {total} profile\\(s\\)_")
            text = "\n".join(lines)
        else:
            text = "📭 No profiles queued\\.\n\nSend `instagram\\_profiles\\.txt` to import\\."

        await _send(ctx, chat.id, text, reply_markup=_back_button())

    async def _cb_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat      = update.effective_chat
        profiles  = self._profiles.all()
        cookies   = self._cookies.list_all()
        plat_count = sum(1 for v in profiles.values() if v)
        total      = self._profiles.total_count()
        state      = "🔄 Download in progress" if self._running else "⏸ Idle"

        lines = [
            f"*📊 Status*\n",
            f"*State:* {_esc(state)}",
            f"*Active platforms:* {plat_count}",
            f"*Profiles queued:* {total}",
            f"*Cookie files:* {len(cookies)}",
        ]
        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_cookies(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat   = update.effective_chat
        stored = self._cookies.list_all()

        lines = ["*🍪 Cookie Files*\n"]
        if stored:
            for name, size in stored:
                kb = size / 1024
                lines.append(f"✅ `{_esc(name)}` — {kb:.1f} KB")
        else:
            lines.append("_No cookies uploaded yet\\._")

        lines += [
            "",
            "*How to upload:*",
            "1\\. Export cookies from your browser",
            "   \\(use a Netscape cookies extension\\)",
            "2\\. Send the file directly to this chat",
            "   e\\.g\\. `instagram\\.com\\_cookies\\.txt`",
        ]
        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if not self._running:
            await _send(ctx, chat.id, "ℹ️ Nothing is running\\.", reply_markup=_back_button())
            return
        self._running = False
        await _send(
            ctx, chat.id,
            "⚠️ Cancel requested\\. Current profile will finish, then the run stops\\.",
            reply_markup=_back_button(),
        )

    async def _cb_allow_here(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        if chat.type == ChatType.PRIVATE:
            await _send(
                ctx, chat.id,
                "ℹ️ Use this button *inside the group* you want to allow\\.",
                reply_markup=_back_button(),
            )
            return
        added = self._groups.allow(chat.id)
        msg   = (
            f"✅ Group `{_esc(str(chat.id))}` added to whitelist\\."
            if added
            else f"ℹ️ Group `{_esc(str(chat.id))}` already whitelisted\\."
        )
        await _send(ctx, chat.id, msg, reply_markup=_back_button())

    async def _cb_groups_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        ids  = self._groups.list_all()
        if not ids:
            await _send(
                ctx, chat.id,
                "📭 No groups whitelisted yet\\.\n"
                "Add the bot to a group and press *✅ Allow This Group*\\.",
                reply_markup=_back_button(),
            )
            return
        lines = ["*⚙️ Whitelisted Groups*\n"]
        for gid in ids:
            lines.append(f"• `{_esc(str(gid))}`")
        await _send(ctx, chat.id, "\n".join(lines), reply_markup=_back_button())

    async def _cb_remove_group_prompt(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat
        ids  = self._groups.list_all()
        if not ids:
            await _send(ctx, chat.id, "No groups to remove\\.", reply_markup=_back_button())
            return
        rows = [
            [InlineKeyboardButton(f"🗑 {gid}", callback_data=f"grp:deny:{gid}")]
            for gid in ids
        ]
        rows.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:main")])
        await _send(
            ctx, chat.id,
            "*Select a group to remove:*",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    # ── /add ─────────────────────────────────────────────────────────────────

    async def cmd_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args or len(ctx.args) < 2:
            await _send(
                ctx, update.effective_chat.id,
                "Usage: `/add <platform> <url>`\n\nPlatforms: `instagram` `tiktok` `facebook` `x`",
            )
            return
        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")
        if platform not in self._cfg.platforms:
            plat_list = _esc(", ".join(self._cfg.platforms))
            await _send(ctx, update.effective_chat.id, f"❌ Unknown platform\\. Use: {plat_list}")
            return
        if not url.startswith("http"):
            await _send(ctx, update.effective_chat.id, "❌ URL must start with `http`\\.")
            return
        plat_cfg = self._cfg.platforms[platform]
        username = Downloader._extract_username(url, plat_cfg) or url
        added    = self._profiles.add(platform, url)
        em       = PLATFORM_EMOJI.get(platform, "📁")
        msg = (
            f"✅ Added `{_esc(username)}` → *{em} {_esc(platform)}*"
            if added else
            "ℹ️ Already in queue\\."
        )
        await _send(ctx, update.effective_chat.id, msg)

    # ── /remove ───────────────────────────────────────────────────────────────

    async def cmd_remove(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args or len(ctx.args) < 2:
            await _send(ctx, update.effective_chat.id, "Usage: `/remove <platform> <url>`")
            return
        platform = ctx.args[0].lower().strip()
        url      = ctx.args[1].strip().rstrip("/")
        if platform not in self._cfg.platforms:
            await _send(ctx, update.effective_chat.id, "❌ Unknown platform\\.")
            return
        removed = self._profiles.remove(platform, url)
        await _send(
            ctx, update.effective_chat.id,
            "🗑 Removed\\." if removed else "❌ URL not found\\.",
        )

    # ── /list ─────────────────────────────────────────────────────────────────

    async def cmd_list(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        # Reuse callback implementation
        await self._cb_list(update, ctx)

    # ── /clear ────────────────────────────────────────────────────────────────

    async def cmd_clear(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args:
            await _send(ctx, update.effective_chat.id, "Usage: `/clear <platform>`")
            return
        platform = ctx.args[0].lower().strip()
        if platform not in self._cfg.platforms:
            await _send(ctx, update.effective_chat.id, "❌ Unknown platform\\.")
            return
        count = self._profiles.clear(platform)
        em    = PLATFORM_EMOJI.get(platform, "📁")
        await _send(
            ctx, update.effective_chat.id,
            f"🗑 Cleared {count} profile\\(s\\) from {em} *{_esc(platform)}*\\.",
        )

    # ── /allowgroup ───────────────────────────────────────────────────────────

    async def cmd_allowgroup(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        chat = update.effective_chat

        # Called with an ID argument: /allowgroup -1001234567
        if ctx.args:
            try:
                gid   = int(ctx.args[0])
                added = self._groups.allow(gid)
                await _send(
                    ctx, chat.id,
                    f"✅ Group `{_esc(str(gid))}` whitelisted\\."
                    if added else
                    f"ℹ️ Group `{_esc(str(gid))}` already in list\\.",
                )
            except ValueError:
                await _send(ctx, chat.id, "❌ Invalid group ID\\. Must be a number\\.")
            return

        # Called inside the group itself with no argument
        if chat.type == ChatType.PRIVATE:
            await _send(
                ctx, chat.id,
                "Use `/allowgroup <group\\_id>` or run this command *inside the group*\\.",
            )
            return

        added = self._groups.allow(chat.id)
        await _send(
            ctx, chat.id,
            f"✅ This group \\(`{_esc(str(chat.id))}`\\) is now whitelisted\\."
            if added else
            "ℹ️ This group is already whitelisted\\.",
        )

    # ── /denygroup ────────────────────────────────────────────────────────────

    async def cmd_denygroup(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        if not ctx.args:
            await _send(ctx, update.effective_chat.id, "Usage: `/denygroup <group\\_id>`")
            return
        try:
            gid     = int(ctx.args[0])
            removed = self._groups.deny(gid)
            await _send(
                ctx, update.effective_chat.id,
                f"🗑 Group `{_esc(str(gid))}` removed\\."
                if removed else
                f"❌ Group `{_esc(str(gid))}` not found\\.",
            )
        except ValueError:
            await _send(ctx, update.effective_chat.id, "❌ Invalid group ID\\.")

    # ── /groups ───────────────────────────────────────────────────────────────

    async def cmd_groups(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update, require_owner=True):
            return
        await self._cb_groups_list(update, ctx)

    # ── /status ───────────────────────────────────────────────────────────────

    async def cmd_status(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_status(update, ctx)

    # ── /cancel ───────────────────────────────────────────────────────────────

    async def cmd_cancel(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_cancel(update, ctx)

    # ── /cookies ──────────────────────────────────────────────────────────────

    async def cmd_cookies(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        await self._cb_cookies(update, ctx)

    # ── /run ──────────────────────────────────────────────────────────────────

    async def cmd_run(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._ok(update):
            return
        mode_str = ctx.args[0].lower() if ctx.args else "both"
        await self._execute_run(update, ctx, MediaMode.from_str(mode_str))

    # ── Core download executor ────────────────────────────────────────────────

    async def _execute_run(
        self,
        update: Update,
        ctx:    ContextTypes.DEFAULT_TYPE,
        mode:   MediaMode,
    ) -> None:
        chat = update.effective_chat

        if self._running:
            await _send(
                ctx, chat.id,
                "⚠️ A download is already running\\. Use *❌ Cancel Run* first\\.",
                reply_markup=_back_button(),
            )
            return

        total = self._profiles.total_count()
        if total == 0:
            await _send(
                ctx, chat.id,
                "📭 No profiles queued\\.\n\n"
                "Send `instagram\\_profiles\\.txt` to the chat to import, or use `/add`\\.",
                reply_markup=_back_button(),
            )
            return

        is_forum   = getattr(chat, "is_forum", False)
        self._running = True

        try:
            await _send(
                ctx, chat.id,
                f"🚀 *Starting* \\[{_esc(mode.label())}\\]\n"
                f"_{total} profile\\(s\\) queued\\.\\.\\._",
            )

            grand_total = 0

            for platform, urls in self._profiles.all().items():
                if not urls:
                    continue
                if not self._running:
                    await _send(ctx, chat.id, "🛑 Run cancelled\\.")
                    return

                plat_cfg = self._cfg.platforms[platform]
                em       = PLATFORM_EMOJI.get(platform, "📁")

                # Platform header in main thread
                await _send(
                    ctx, chat.id,
                    f"\n📂 *{em} {_esc(plat_cfg.label)}*",
                )

                for url in urls:
                    if not self._running:
                        await _send(ctx, chat.id, "🛑 Run cancelled\\.")
                        return

                    username = Downloader._extract_username(url, plat_cfg) or url

                    # ── Get or create forum topic ─────────────────────────
                    thread_id: int | None = None
                    if is_forum:
                        thread_id = await self._get_or_create_topic(
                            ctx, chat.id, platform, username
                        )

                    # Announce in topic (or main thread)
                    await _send(
                        ctx, chat.id,
                        f"▶️ `{_esc(username)}`",
                        thread_id=thread_id,
                    )

                    # ── Download ──────────────────────────────────────────
                    result = await self._dl.download_user(url, plat_cfg, mode)

                    if result.skipped:
                        await _send(
                            ctx, chat.id,
                            f"⚠️ Skipped: _{_esc(result.skip_reason)}_",
                            thread_id=thread_id,
                        )
                        continue

                    # Report archive status per subfolder
                    for sub in result.results:
                        line = (
                            f"  `\\[{_esc(sub.subfolder)}\\]` "
                            f"archive: {_esc(sub.archive_action)}"
                        )
                        if sub.error:
                            line += f"\n  ⚠️ `{_esc(sub.error[:200])}`"
                        await _send(ctx, chat.id, line, thread_id=thread_id)

                    new_count = result.total_new
                    grand_total += new_count

                    await _send(
                        ctx, chat.id,
                        f"✅ `{_esc(username)}` — {new_count} new file\\(s\\)",
                        thread_id=thread_id,
                    )

                    # ── Deliver files ─────────────────────────────────────
                    all_new: list[Path] = [
                        f for sub in result.results for f in sub.new_files
                    ]
                    await self._deliver_files(ctx, chat.id, all_new, thread_id)

            await _send(
                ctx, chat.id,
                f"🏁 *All done* — {grand_total} new file\\(s\\) total\\.",
                reply_markup=_back_button(),
            )

        except Exception as exc:
            logger.exception("Unexpected error during run: %s", exc)
            await _send(
                ctx, chat.id,
                f"🔥 Internal error: `{_esc(str(exc)[:200])}`",
                reply_markup=_back_button(),
            )
        finally:
            self._running = False

    # ── Forum topic management ────────────────────────────────────────────────

    async def _get_or_create_topic(
        self,
        ctx:      ContextTypes.DEFAULT_TYPE,
        chat_id:  int,
        platform: str,
        username: str,
    ) -> int | None:
        """Return existing topic thread_id or create a new one."""
        stored = self._topics.get(chat_id, platform, username)
        if stored is not None:
            return stored

        em         = PLATFORM_EMOJI.get(platform, "📁")
        color      = TOPIC_COLORS.get(platform, 0x6FB9F0)
        topic_name = f"{em} {username}"[:128]

        try:
            topic = await ctx.bot.create_forum_topic(
                chat_id=chat_id,
                name=topic_name,
                icon_color=color,
            )
            thread_id = topic.message_thread_id
            self._topics.set(chat_id, platform, username, thread_id)
            logger.info("Created topic '%s' thread_id=%d", topic_name, thread_id)
            return thread_id
        except BadRequest as exc:
            logger.warning("Could not create topic '%s': %s", topic_name, exc)
            return None
        except TelegramError as exc:
            logger.error("Topic creation error: %s", exc)
            return None

    # ── File delivery ─────────────────────────────────────────────────────────

    async def _deliver_files(
        self,
        ctx:       ContextTypes.DEFAULT_TYPE,
        chat_id:   int,
        files:     list[Path],
        thread_id: int | None,
    ) -> None:
        cap   = self._cfg.max_send_files
        limit = self._cfg.max_file_size_mb
        sent  = 0

        for path in files:
            if sent >= cap:
                remaining = len(files) - sent
                if remaining > 0:
                    await _send(
                        ctx, chat_id,
                        f"ℹ️ {remaining} more file\\(s\\) saved to disk \\(cap of {cap} reached\\)\\.",
                        thread_id=thread_id,
                    )
                break

            try:
                size_mb = path.stat().st_size / (1024 * 1024)
            except OSError:
                continue

            if size_mb > limit:
                await _send(
                    ctx, chat_id,
                    f"⚠️ `{_esc(path.name)}` is {size_mb:.1f} MB — "
                    f"too large for Telegram \\(max {limit} MB\\), saved to disk\\.",
                    thread_id=thread_id,
                )
                continue

            ext = path.suffix.lstrip(".").lower()
            try:
                with path.open("rb") as fh:
                    if ext in self._cfg.video_exts:
                        await ctx.bot.send_video(
                            chat_id=chat_id,
                            video=fh,
                            message_thread_id=thread_id,
                        )
                    elif ext in self._cfg.photo_exts:
                        await ctx.bot.send_photo(
                            chat_id=chat_id,
                            photo=fh,
                            message_thread_id=thread_id,
                        )
                    else:
                        await ctx.bot.send_document(
                            chat_id=chat_id,
                            document=fh,
                            message_thread_id=thread_id,
                        )
                sent += 1
            except TelegramError:
                # Fallback: send as document
                try:
                    with path.open("rb") as fh:
                        await ctx.bot.send_document(
                            chat_id=chat_id,
                            document=fh,
                            message_thread_id=thread_id,
                        )
                    sent += 1
                except TelegramError as exc:
                    logger.warning("Could not send %s: %s", path.name, exc)

        logger.info("Delivered %d/%d file(s) to chat=%d thread=%s", sent, len(files), chat_id, thread_id)

    # ── Document upload handler ───────────────────────────────────────────────

    async def handle_document(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle cookie file and bulk profile imports sent as documents."""
        chat = update.effective_chat

        # Group: must be whitelisted. Cookie/profile upload: owner only.
        if not self._ok(update, require_owner=True):
            return

        doc  = update.message.document
        name = (doc.file_name or "").strip()

        if not name:
            return

        # ── Cookie file ────────────────────────────────────────────────────
        if self._cookies.is_valid_name(name):
            try:
                tg_file = await ctx.bot.get_file(doc.file_id)
                raw     = await tg_file.download_as_bytearray()
                if not raw:
                    await _send(ctx, chat.id, "❌ Received empty file\\.")
                    return
                self._cookies.save(name, bytes(raw))
                await _send(
                    ctx, chat.id,
                    f"🍪 Cookie saved: `{_esc(name)}` \\({len(raw):,} bytes\\)\n\n"
                    f"Use *▶️ Run* to start downloading\\.",
                    reply_markup=_main_menu(self._user_is_owner(update)),
                )
            except (TelegramError, OSError) as exc:
                logger.error("Cookie upload failed: %s", exc)
                await _send(ctx, chat.id, f"❌ Failed to save cookie: `{_esc(str(exc)[:150])}`")
            return

        # ── Bulk profile import ────────────────────────────────────────────
        for platform in self._cfg.platforms:
            if name == f"{platform}_profiles.txt":
                try:
                    tg_file = await ctx.bot.get_file(doc.file_id)
                    raw     = await tg_file.download_as_bytearray()
                    text    = raw.decode(errors="replace")
                    urls    = [
                        line.strip().rstrip("/")
                        for line in text.splitlines()
                        if line.strip().startswith("http")
                    ]
                    if not urls:
                        await _send(ctx, chat.id, "❌ No valid URLs found in the file\\.")
                        return
                    added = self._profiles.add_bulk(platform, urls)
                    total = len(self._profiles.get(platform))
                    em    = PLATFORM_EMOJI.get(platform, "📁")
                    await _send(
                        ctx, chat.id,
                        f"📋 Added *{added}* profile\\(s\\) to "
                        f"{em} *{_esc(platform)}* \\({total} total\\)\n\n"
                        f"Press *▶️ Run* to start downloading\\.",
                        reply_markup=_main_menu(self._user_is_owner(update)),
                    )
                except (TelegramError, OSError) as exc:
                    logger.error("Bulk import failed: %s", exc)
                    await _send(ctx, chat.id, f"❌ Import failed: `{_esc(str(exc)[:150])}`")
                return

        await _send(
            ctx, chat.id,
            f"❓ Unrecognised file: `{_esc(name)}`\n\n"
            "*Expected files:*\n"
            "`instagram\\.com\\_cookies\\.txt`\n"
            "`tiktok\\.com\\_cookies\\.txt`\n"
            "`instagram\\_profiles\\.txt`\n"
            "`tiktok\\_profiles\\.txt`  etc\\.",
        )


# ── Bot commands list (for Telegram "/" menu) ─────────────────────────────────

BOT_COMMANDS: list[BotCommand] = [
    BotCommand("start",       "Show main menu"),
    BotCommand("run",         "Start download [photos|videos|both]"),
    BotCommand("add",         "Add a profile: /add <platform> <url>"),
    BotCommand("remove",      "Remove a profile: /remove <platform> <url>"),
    BotCommand("list",        "Show all queued profiles"),
    BotCommand("clear",       "Clear a platform: /clear <platform>"),
    BotCommand("status",      "Show queue summary and run state"),
    BotCommand("cookies",     "List uploaded cookie files"),
    BotCommand("cancel",      "Stop an ongoing download"),
    BotCommand("allowgroup",  "Whitelist a group: /allowgroup [group_id]"),
    BotCommand("denygroup",   "Remove a group: /denygroup <group_id>"),
    BotCommand("groups",      "List all whitelisted groups"),
]
