#!/usr/bin/env python3
"""
KissuDLBot - single-file Telegram bot
Required environment variables (in .env or env):
- BOT_TOKEN
- API_ID
- API_HASH
- ADMIN_ID
- FSUB_CHANNEL
- FSUB_CHANNEL_LINK

Optional env vars:
- TELEGRAM_UPLOAD_LIMIT_MB (default 50)
- DOWNLOAD_DIR (default downloads)
- DB_PATH (default kissu.db)
- TIMEZONE (default Asia/Kolkata)

Notes:
- Requires ffmpeg on PATH for yt-dlp merging.
- Install dependencies: pip install -r requirements.txt
"""

import os
import re
import sys
import time
import logging
import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import subprocess
from typing import List

from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, Message
from pyrogram.errors import UserNotParticipant, FloodWait
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import yt_dlp
import requests
import instaloader

# ----------------- Config / Env -----------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
ADMIN_ID = os.getenv("ADMIN_ID")
FSUB_CHANNEL = os.getenv("FSUB_CHANNEL")
FSUB_CHANNEL_LINK = os.getenv("FSUB_CHANNEL_LINK")

# Validate required envs
if not all([BOT_TOKEN, API_ID, API_HASH, ADMIN_ID, FSUB_CHANNEL, FSUB_CHANNEL_LINK]):
    missing = [k for k, v in (("BOT_TOKEN", BOT_TOKEN), ("API_ID", API_ID), ("API_HASH", API_HASH), ("ADMIN_ID", ADMIN_ID), ("FSUB_CHANNEL", FSUB_CHANNEL), ("FSUB_CHANNEL_LINK", FSUB_CHANNEL_LINK)) if not v]
    print("Missing required env vars:", missing)
    sys.exit(1)

try:
    API_ID = int(API_ID)
    ADMIN_ID = int(ADMIN_ID)
except Exception:
    print("API_ID and ADMIN_ID must be integers")
    sys.exit(1)

TELEGRAM_UPLOAD_LIMIT_MB = int(os.getenv("TELEGRAM_UPLOAD_LIMIT_MB", "50"))
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
DB_PATH = os.getenv("DB_PATH", "kissu.db")
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

# ----------------- Instaloader login (optional) -----------------
# Instaloader anonymous mode se public posts/reels download kar leta hai, lekin
# private accounts, highlights, ya heavy usage pe Instagram jaldi rate-limit
# karta hai. Agar ek logged-in session chahiye:
#   1. Apne PC pe ek baar chalao: instaloader --login=<IG_USERNAME>
#      (isse ek session file ~/.config/instaloader/ me ban jayegi)
#   2. Wo file server pe upload karke IG_SESSION_FILE me uska path do,
#      aur IG_USERNAME me wahi username do.
IG_USERNAME = os.getenv("IG_USERNAME")
IG_SESSION_FILE = os.getenv("IG_SESSION_FILE")

# ----------------- Cobalt (universal free downloader API) -----------------
# Cobalt (https://cobalt.tools) YouTube/Insta/X/TikTok/Reddit/etc ke liye ek
# free hosted API deta hai — last-resort fallback jab baaki sab engines fail
# ho jayein. Public instance ki URL/limits samay ke saath badal sakti hain,
# isliye env se override karne ka option diya gaya hai. Kuch instances API
# key maangte hain — COBALT_API_KEY set karo agar tumhare instance ko chahiye.
COBALT_API_URL = os.getenv("COBALT_API_URL", "https://api.cobalt.tools/api/json")
COBALT_API_KEY = os.getenv("COBALT_API_KEY")

# ----------------- Rate limiting -----------------
# Har user ke recent-download-request timestamps track karte hain (sliding
# window). Sirf 2 parallel downloads chalte hain (_EXEC max_workers=2), isliye
# spam-requests download-queue ko jaam kar sakte hain — ye us se bachata hai.
download_times = {}  # { user_id: [timestamp1, timestamp2, ...] }
RATELIMIT_COUNT = 3
RATELIMIT_WINDOW = 60   # seconds
RATELIMIT_WAIT = 30     # seconds

# ----------------- Cookies (for private Instagram / X content) -----------------
# Two ways to provide login cookies, so downloads work for accounts (like a
# private Instagram) that need to be logged in to view:
#   COOKIES_FILE    - path to an existing cookies.txt already on disk
#   COOKIES_CONTENT - the raw cookies.txt text, pasted directly as a Railway
#                      env var. Easier on Railway since you don't need a
#                      volume or to commit the file to your repo.
# If both are unset, yt-dlp just downloads public content only.
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

COOKIES_FILE = os.getenv("COOKIES_FILE")
COOKIES_CONTENT = os.getenv("COOKIES_CONTENT")

if not COOKIES_FILE and COOKIES_CONTENT:
    COOKIES_FILE = os.path.join(DOWNLOAD_DIR, ".cookies.txt")
    try:
        with open(COOKIES_FILE, "w") as f:
            f.write(COOKIES_CONTENT)
    except Exception as e:
        print("Failed to write cookies file from COOKIES_CONTENT:", e)
        COOKIES_FILE = None

# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("Kissudl")
# reduce noise from libraries
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("yt_dlp").setLevel(logging.WARNING)

# ----------------- DB -----------------

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, name TEXT, downloads INTEGER DEFAULT 0)")
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    conn.commit()
    return conn

DB = init_db()
DB_LOCK = asyncio.Lock()

def add_user_sync(user_id: int, name: str):
    try:
        DB.execute("INSERT OR IGNORE INTO users (user_id, name) VALUES (?, ?)", (user_id, name))
        DB.commit()
    except Exception:
        logger.exception("add_user failed")

async def add_user(user_id: int, name: str):
    async with DB_LOCK:
        add_user_sync(user_id, name)

def add_download_sync(user_id: int):
    try:
        DB.execute("UPDATE users SET downloads = downloads + 1 WHERE user_id = ?", (user_id,))
        DB.commit()
    except Exception:
        logger.exception("add_download failed")

async def add_download(user_id: int):
    async with DB_LOCK:
        add_download_sync(user_id)

async def get_user(user_id: int):
    async with DB_LOCK:
        cur = DB.execute("SELECT user_id, name, downloads FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()

async def get_all_user_ids():
    async with DB_LOCK:
        cur = DB.execute("SELECT user_id FROM users")
        return [r[0] for r in cur.fetchall()]

async def get_stats():
    async with DB_LOCK:
        cur = DB.execute("SELECT COUNT(*), COALESCE(SUM(downloads),0) FROM users")
        return cur.fetchone()

def set_setting_sync(key: str, value: str):
    try:
        DB.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        DB.commit()
    except Exception:
        logger.exception("set_setting failed")

async def set_setting(key: str, value):
    async with DB_LOCK:
        set_setting_sync(key, value)

def load_all_settings_sync() -> dict:
    try:
        cur = DB.execute("SELECT key, value FROM settings")
        return dict(cur.fetchall())
    except Exception:
        logger.exception("load_all_settings failed")
        return {}

# ----------------- Downloader -----------------

_EXEC = ThreadPoolExecutor(max_workers=2)

def _make_ydl_opts(outtmpl: str, progress_hook: Optional[Callable] = None):
    opts = {
        "format": "bestvideo+bestaudio/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "noplaylist": True,
        "merge_output_format": "mp4",
    }
    if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE  # from COOKIES_FILE or COOKIES_CONTENT env var
    if progress_hook:
        opts["progress_hooks"] = [progress_hook]
    return opts

async def download_with_yt_dlp(url: str, download_dir: str, filename_prefix: str = "", progress_callback: Optional[Callable] = None) -> Tuple[str, dict]:
    loop = asyncio.get_running_loop()
    os.makedirs(download_dir, exist_ok=True)

    def progress_hook(d):
        if progress_callback:
            try:
                loop.call_soon_threadsafe(progress_callback, d)
            except Exception:
                pass

    outtmpl = os.path.join(download_dir, f"{filename_prefix}%(id)s.%(ext)s")
    opts = _make_ydl_opts(outtmpl, progress_hook)

    def _run():
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            fname = ydl.prepare_filename(info)
            return fname, info

    return await loop.run_in_executor(_EXEC, _run)


async def estimate_filesize(url: str) -> Optional[int]:
    """Metadata-only extract (download=False) taaki upload-limit check download
    shuru karne se PEHLE ho sake. yt_dlp ka filesize estimate exact nahi hota
    hamesha (kabhi filesize_approx milta hai, kabhi bilkul nahi) — is wajah se
    ye best-effort hai: None milne pe caller download karke hi actual size
    check karega (jo pehle se ho raha tha)."""
    loop = asyncio.get_running_loop()
    opts = {"quiet": True, "noplaylist": True, "skip_download": True}
    if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE

    def _run():
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get("filesize") or info.get("filesize_approx")

    try:
        return await loop.run_in_executor(_EXEC, _run)
    except Exception:
        return None

# ----------------- Multi-Engine Downloader -----------------
# Ek hi link ke liye kai downloader "engines" try karte hain, order mein.
# Pehla jo kaam kar jaye (files return kare) wahi use hota hai; agar wo fail
# ho (exception ya khaali result) to automatically agla engine try hota hai.
# Isse yt-dlp akela jo miss kar deta hai (jaise Instagram photo-carousels,
# private posts, ya kuch niche/regional sites) wo instaloader/gallery-dl/
# free public APIs se cover ho jata hai.

IG_SHORTCODE_RE = re.compile(r"instagram\.com/(?:p|reel|reels|tv)/([A-Za-z0-9_-]+)")

_IG_LOADER: Optional["instaloader.Instaloader"] = None

def _get_instaloader() -> "instaloader.Instaloader":
    """Instaloader instance ek hi baar banate hain (lazy singleton) taaki
    session baar baar reload na ho."""
    global _IG_LOADER
    if _IG_LOADER is None:
        L = instaloader.Instaloader(
            download_videos=True,
            download_video_thumbnails=False,
            download_geotags=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            post_metadata_txt_pattern="",
            quiet=True,
        )
        if IG_SESSION_FILE and IG_USERNAME and os.path.isfile(IG_SESSION_FILE):
            try:
                L.load_session_from_file(IG_USERNAME, IG_SESSION_FILE)
                logger.info("Instaloader: logged-in session load ho gaya.")
            except Exception:
                logger.warning("Instaloader session load fail hua, anonymous mode use hoga.")
        _IG_LOADER = L
    return _IG_LOADER


async def download_with_instaloader(url: str, download_dir: str, user_id: int) -> List[str]:
    """Instagram posts/reels ke liye — carousels (multiple photos/videos) bhi
    handle karta hai, jo yt-dlp aksar miss kar deta hai."""
    m = IG_SHORTCODE_RE.search(url)
    if not m:
        return []
    shortcode = m.group(1)
    loop = asyncio.get_running_loop()

    def _run():
        L = _get_instaloader()
        target_dir = os.path.join(download_dir, f"ig_{user_id}_{shortcode}")
        os.makedirs(target_dir, exist_ok=True)
        post = instaloader.Post.from_shortcode(L.context, shortcode)
        L.download_post(post, target=target_dir)
        files = []
        for fn in sorted(os.listdir(target_dir)):
            if fn.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".mp4")):
                files.append(os.path.join(target_dir, fn))
        return files

    return await loop.run_in_executor(_EXEC, _run)


async def download_with_gallery_dl(url: str, download_dir: str, user_id: int) -> List[str]:
    """gallery-dl ek generic multi-site downloader hai — Instagram, Twitter/X,
    Reddit, Pinterest, Tumblr, DeviantArt, Facebook photos, aur bahut saari
    aur sites support karta hai. yt-dlp/instaloader fail hone par isko
    fallback ki tarah use karte hain."""
    loop = asyncio.get_running_loop()
    target_dir = os.path.join(download_dir, f"gdl_{user_id}_{int(time.time() * 1000)}")
    os.makedirs(target_dir, exist_ok=True)

    cmd = ["gallery-dl", "--directory", target_dir, "-q"]
    if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
        cmd += ["--cookies", COOKIES_FILE]
    cmd.append(url)

    def _run():
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            if proc.returncode != 0:
                logger.warning(f"gallery-dl exit {proc.returncode}: {(proc.stderr or '')[:300]}")
        except FileNotFoundError:
            logger.warning("gallery-dl command nahi mila (requirements.txt se install ho gaya hai check karo).")
            return []
        except subprocess.TimeoutExpired:
            logger.warning("gallery-dl timeout ho gaya.")
        files = []
        for root, _dirs, fnames in os.walk(target_dir):
            for fn in fnames:
                if fn.lower().endswith((".json", ".part")):
                    continue
                files.append(os.path.join(root, fn))
        return files

    return await loop.run_in_executor(_EXEC, _run)


async def download_with_ytdlp_multi(url: str, download_dir: str, user_id: int) -> List[str]:
    """Existing yt-dlp downloader ko multi-file interface me wrap karta hai."""
    fname, _info = await download_with_yt_dlp(url, download_dir, filename_prefix=f"{user_id}_")
    return [fname] if fname and os.path.exists(fname) else []


async def download_with_tikwm(url: str, download_dir: str, user_id: int) -> List[str]:
    """TikTok ke liye free public API (tikwm.com, no-key) — no-watermark video
    deta hai. Isse pehle try karte hain kyunki yt-dlp TikTok pe kabhi kabhi
    IP/region ki wajah se block ho jata hai."""
    if not TT_RE.search(url):
        return []
    loop = asyncio.get_running_loop()

    def _run():
        try:
            r = requests.get("https://www.tikwm.com/api/", params={"url": url}, timeout=20)
            data = r.json()
        except Exception:
            return []
        if data.get("code") != 0:
            return []
        d = data.get("data") or {}
        play_url = d.get("hdplay") or d.get("play")
        if not play_url:
            return []
        try:
            vid = requests.get(play_url, timeout=120)
            vid.raise_for_status()
        except Exception:
            return []
        fname = os.path.join(download_dir, f"tt_{user_id}_{int(time.time() * 1000)}.mp4")
        with open(fname, "wb") as f:
            f.write(vid.content)
        return [fname]

    return await loop.run_in_executor(_EXEC, _run)


async def download_with_cobalt(url: str, download_dir: str, user_id: int) -> List[str]:
    """Cobalt API — universal last-resort fallback (YouTube/Insta/X/TikTok/
    Reddit/etc). Public instance ki availability/limits samay ke saath badal
    sakti hai, isliye ye chain me sabse aakhri me try hota hai."""
    loop = asyncio.get_running_loop()

    def _run():
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if COBALT_API_KEY:
            headers["Authorization"] = f"Api-Key {COBALT_API_KEY}"
        try:
            resp = requests.post(COBALT_API_URL, json={"url": url}, headers=headers, timeout=30)
            data = resp.json()
        except Exception:
            return []

        status = data.get("status")
        urls = []
        if status in ("stream", "redirect", "tunnel") and data.get("url"):
            urls = [data["url"]]
        elif status == "picker":
            urls = [item["url"] for item in data.get("picker", []) if item.get("url")]

        files = []
        for i, u in enumerate(urls):
            try:
                r = requests.get(u, timeout=180)
                r.raise_for_status()
                ctype = r.headers.get("Content-Type", "")
                ext = ".mp4" if "video" in ctype else (".jpg" if "image" in ctype else ".bin")
                fname = os.path.join(download_dir, f"cb_{user_id}_{int(time.time() * 1000)}_{i}{ext}")
                with open(fname, "wb") as f:
                    f.write(r.content)
                files.append(fname)
            except Exception:
                continue
        return files

    return await loop.run_in_executor(_EXEC, _run)


def pick_engines(url: str) -> list:
    """URL dekh kar sahi order me engines chunta hai — platform-specific
    engine pehle, fir generic fallbacks."""
    if INST_RE.search(url):
        return [download_with_instaloader, download_with_gallery_dl, download_with_ytdlp_multi, download_with_cobalt]
    if TT_RE.search(url):
        return [download_with_tikwm, download_with_ytdlp_multi, download_with_gallery_dl, download_with_cobalt]
    if YT_RE.search(url):
        return [download_with_ytdlp_multi, download_with_cobalt]
    if X_RE.search(url) or PIN_RE.search(url) or REDDIT_RE.search(url) or FB_RE.search(url):
        return [download_with_gallery_dl, download_with_ytdlp_multi, download_with_cobalt]
    # Koi bhi aur/unknown site — sabse generic engines try karo.
    return [download_with_gallery_dl, download_with_ytdlp_multi, download_with_cobalt]


async def download_media(url: str, download_dir: str, user_id: int) -> List[str]:
    """Engines ko order me try karta hai, jo pehla non-empty result de wahi
    return hota hai. Sab fail ho jayein to last exception raise karta hai."""
    last_err: Optional[Exception] = None
    for engine in pick_engines(url):
        try:
            files = await engine(url, download_dir, user_id)
            if files:
                return files
        except Exception as e:
            last_err = e
            logger.warning(f"{engine.__name__} fail hua: {e}")
            continue
    if last_err:
        raise last_err
    raise RuntimeError("Koi bhi engine is link ko download nahi kar paya.")

# ----------------- Utils -----------------

def human_size(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"

def cleanup_old_files(path: str, days: int = 3):
    try:
        cutoff = time.time() - days * 86400
        for fn in os.listdir(path):
            fp = os.path.join(path, fn)
            try:
                if os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                    os.remove(fp)
            except Exception:
                pass
    except Exception:
        pass

# ----------------- URL matching -----------------
# NOTE: single backslashes here (\w, \.) — these are regex shorthand for
# "word character" and "literal dot". Doubling them (\\w, \\.) breaks the
# match entirely, which was the previous bug: the bot could never detect
# a real Instagram/X link.
URL_RE = re.compile(r"https?://[\w./?=&%-]+")
INST_RE = re.compile(r"(instagram\.com|instagr\.am)")
X_RE = re.compile(r"(x\.com|twitter\.com|t\.co)")
YT_RE = re.compile(r"(youtube\.com|youtu\.be)")
TT_RE = re.compile(r"(tiktok\.com)")
PIN_RE = re.compile(r"(pinterest\.com|pin\.it)")
REDDIT_RE = re.compile(r"(reddit\.com|redd\.it)")
FB_RE = re.compile(r"(facebook\.com|fb\.watch)")

def is_command_text(text: Optional[str]) -> bool:
    return bool(text and text.startswith("/"))

# custom filter: matches any private text message that is NOT a slash-command.
# (filters.command() requires at least one command name — it cannot be called
# empty — so a plain regex/text check is used instead of ~filters.command())
not_command_filter = filters.create(lambda _, __, m: not is_command_text(m.text))

# ----------------- Bot -----------------
# NOTE: the Client is intentionally NOT constructed here at module level.
# It's created inside _run(), after asyncio.run() has started the actual
# event loop. Building it here (before any loop is running) is what caused
# its internal dispatcher tasks to bind to a stray loop that never actually
# runs — messages would go in and nothing would come out, silently.

# keyboards
main_kb = ReplyKeyboardMarkup([["Help", "Account"], ["About"]], resize_keyboard=True)

# in-memory settings the admin can change at runtime via /admin, without a redeploy.
# these are persisted to the sqlite `settings` table so they survive restarts —
# defaults come from env vars, then get overridden by whatever's saved in DB.
_bot_settings = {
    "fsub_enabled": True,
    "upload_limit_mb": TELEGRAM_UPLOAD_LIMIT_MB,
    "fsub_channel_link": FSUB_CHANNEL_LINK,
    "fsub_channel_id": FSUB_CHANNEL,
}

_saved_settings = load_all_settings_sync()
if "fsub_enabled" in _saved_settings:
    _bot_settings["fsub_enabled"] = _saved_settings["fsub_enabled"] == "1"
if "upload_limit_mb" in _saved_settings:
    try:
        _bot_settings["upload_limit_mb"] = int(_saved_settings["upload_limit_mb"])
    except ValueError:
        pass
if "fsub_channel_link" in _saved_settings:
    _bot_settings["fsub_channel_link"] = _saved_settings["fsub_channel_link"]
if "fsub_channel_id" in _saved_settings:
    _bot_settings["fsub_channel_id"] = _saved_settings["fsub_channel_id"]

# ----------------- Editable Messages -----------------
# every user-facing message text below can be changed live from the Admin
# Panel (📝 Edit Messages) without touching code or redeploying. Defaults
# are used until the admin overrides one; overrides are persisted in the
# same `settings` table (key = "msg_<name>") so they survive restarts.
DEFAULT_MESSAGES = {
    "welcome_msg": "`Insta X YT TikTok Pinterest Reddit FB` ka koi 1 **Link** 🔗 do.",
    "verify_msg": "🗝️",
    "verified_msg": "✅",
    "gm_msg": "☀️",
    "help_msg": (
        "🤖 Help:\n"
        "🔗 Instagram/X/YouTube/TikTok/Pinterest/Reddit/Facebook links bhejo.\n"
        "⬇️ Mai download karke file bhej dunga (agar Telegram allow kare).\n"
        "🖼️ Instagram carousel (multiple photos/videos) bhi sabhi files bhej deta hai.\n"
        "📞 Agar bahut badi file hui to contact admin @KissuADMIN.\n"
    ),
    "about_msg": (
        "ℹ️ About\n\n"
        "👨‍💻 Developer: @KissuADMIN\n"
        "📸 Instagram: @i07444\n"
        "🐦 X: @456bug\n\n"
        "🤖 Code assist: Claude (Anthropic)"
    ),
}

# order + labels used to build the "Edit Messages" menu in the admin panel
MESSAGE_LABELS = {
    "welcome_msg": "👋 Welcome Message",
    "verify_msg": "🗝️ Verify / Join Message",
    "verified_msg": "🎉 Verified Success Message",
    "gm_msg": "☀️ Good Morning Message",
    "help_msg": "❓ Help Message",
    "about_msg": "ℹ️ About Message",
}

_bot_messages = dict(DEFAULT_MESSAGES)
for _key in DEFAULT_MESSAGES:
    _db_key = f"msg_{_key}"
    if _db_key in _saved_settings:
        _bot_messages[_key] = _saved_settings[_db_key]

def render_msg(key: str, **kwargs) -> str:
    """Current (possibly admin-edited) text for `key`, with {placeholders}
    (like {name}) filled in. Falls back to the raw template if an admin's
    edited text has a stray/mismatched brace, so a typo in the admin panel
    can never crash the bot."""
    template = _bot_messages.get(key, DEFAULT_MESSAGES.get(key, ""))
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def admin_messages_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(label, callback_data=f"admin_edit_msg_{key}")] for key, label in MESSAGE_LABELS.items()]
    rows.append([InlineKeyboardButton("🔄 Reset All", callback_data="admin_reset_all_msgs")])
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="admin_back_main"), InlineKeyboardButton("🔚 Quit", callback_data="admin_quit")])
    return InlineKeyboardMarkup(rows)

def admin_edit_msg_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Reset to Default", callback_data=f"admin_reset_msg_{key}")],
        [InlineKeyboardButton("🔙 Cancel", callback_data="admin_messages_menu")],
    ])

def fsub_join_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Channel Join Karo", url=_bot_settings["fsub_channel_link"])],
        [InlineKeyboardButton("✅ Verify", callback_data="verify_fsub")],
    ])

def admin_main_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast"), InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings"), InlineKeyboardButton("📝 Edit Messages", callback_data="admin_messages_menu")],
        [InlineKeyboardButton("🧹 Cleanup Now", callback_data="admin_cleanup")],
        [InlineKeyboardButton("🔚 Quit", callback_data="admin_quit")],
    ])

def admin_settings_kb() -> InlineKeyboardMarkup:
    fsub_status = "🔔 ON" if _bot_settings["fsub_enabled"] else "🔕 OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Force Sub: {fsub_status}", callback_data="admin_toggle_fsub")],
        [InlineKeyboardButton("🆔 FSUB Channel ID", callback_data="admin_set_fsub_id")],
        [InlineKeyboardButton("🔗 FSUB Link", callback_data="admin_set_fsub_link")],
        [InlineKeyboardButton("📦 Upload Limit", callback_data="admin_set_limit")],
        [InlineKeyboardButton("🔙 Back", callback_data="admin_back_main"), InlineKeyboardButton("🔚 Quit", callback_data="admin_quit")],
    ])

def admin_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Cancel", callback_data="admin_back_main")]])

# in-memory admin state — generalized so any settings prompt (broadcast, upload
# limit, fsub link, ...) can wait for the admin's next text message.
_admin_state = {"awaiting": None}

# this filter only matches when we are actually waiting for admin text input.
# Putting the state check INSIDE the filter (instead of inside the handler body)
# matters: if the filter doesn't match, Pyrogram falls through to the next
# handler in the group. Previously this handler always matched for any admin
# text message and swallowed it — including the admin's own download links —
# because Pyrogram stops at the first matching handler in a group.
awaiting_admin_input_filter = filters.create(lambda _, __, m: _admin_state.get("awaiting") is not None)

async def check_fsub(client: Client, user_id: int) -> bool:
    channel = _bot_settings["fsub_channel_id"]
    if not _bot_settings["fsub_enabled"] or not channel:
        return True
    try:
        chat = int(channel)
    except (TypeError, ValueError):
        chat = channel  # @username form
    try:
        await client.get_chat_member(chat, user_id)
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        # Fail-CLOSED: koi bhi error (bot admin nahi hai, wrong channel ID, etc.)
        # ka matlab hai "verify nahi ho paya" -> user ko not-joined treat karo.
        # Fail-open (return True) ek misconfiguration se FSUB silently bypass
        # kar deta — production me ye zyada risky hai.
        logger.warning(f"FSUB check fail hua (fail-closed): {e}")
        return False

async def start_handler(client: Client, message: Message):
    uid = message.from_user.id
    name = message.from_user.first_name or "User"
    await add_user(uid, name)
    if not await check_fsub(client, uid):
        await message.reply_text(
            render_msg("verify_msg", name=name),
            reply_markup=fsub_join_kb(),
        )
        return
    await message.reply_text(render_msg("welcome_msg", name=name), reply_markup=main_kb)

async def help_handler(client: Client, message: Message):
    await message.reply_text(render_msg("help_msg"))

async def about_handler(client: Client, message: Message):
    await message.reply_text(render_msg("about_msg"))

async def account_handler(client: Client, message: Message):
    row = await get_user(message.from_user.id)
    if not row:
        await message.reply_text("⚠️ No account record. Send /start first.")
        return
    uid, name, downloads = row
    await message.reply_text(f"👤 Name: {name}\n🆔 ID: `{uid}`\n⬇️ Downloads: {downloads}")

async def admin_handler(client: Client, message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply_text("❌")
        return
    _admin_state["awaiting"] = None
    await message.reply_text("🛠 Admin Panel:", reply_markup=admin_main_kb())

async def callback_handler(client: Client, query):
    data = query.data

    # ---- available to any user: FSUB verification ----
    if data == "verify_fsub":
        uid = query.from_user.id
        if await check_fsub(client, uid):
            name = query.from_user.first_name or "User"
            await query.answer("✅ Verified! Welcome 🎉", show_alert=True)
            try:
                await query.message.edit_text(render_msg("verified_msg", name=name))
            except Exception:
                pass
            await client.send_message(
                uid,
                render_msg("welcome_msg", name=name),
                reply_markup=main_kb,
            )
        else:
            await query.answer("🚫 Abhi tak join nahi kiya bhai. Pehle channel join karo.", show_alert=True)
        return

    # ---- everything below is admin-only ----
    if query.from_user.id != ADMIN_ID:
        await query.answer("❌", show_alert=True)
        return

    if data == "admin_stats":
        total_users, total_dl = await get_stats()
        await query.answer(f"👥 Users: {total_users} | ⬇️ Downloads: {total_dl}", show_alert=True)

    elif data == "admin_broadcast":
        _admin_state["awaiting"] = "broadcast"
        await query.message.edit_text("📢 Broadcast message ab bhejo (text):", reply_markup=admin_cancel_kb())
        await query.answer()

    elif data == "admin_settings":
        await query.message.edit_text("⚙️ Settings:", reply_markup=admin_settings_kb())
        await query.answer()

    elif data == "admin_messages_menu":
        _admin_state["awaiting"] = None
        await query.message.edit_text("📝 Kaunsa message edit karna hai?", reply_markup=admin_messages_kb())
        await query.answer()

    elif data.startswith("admin_edit_msg_"):
        key = data[len("admin_edit_msg_"):]
        if key not in DEFAULT_MESSAGES:
            await query.answer("❓ Unknown message", show_alert=True)
        else:
            _admin_state["awaiting"] = f"msg_{key}"
            label = MESSAGE_LABELS[key]
            current = _bot_messages.get(key, DEFAULT_MESSAGES[key])
            hint = "\n\n💡 Tip: `{name}` likhoge to wahan user ka naam apne aap aa jayega." if "{name}" in DEFAULT_MESSAGES[key] else ""
            await query.message.edit_text(
                f"✏️ {label}\n\nAbhi ye hai:\n{current}\n\nNaya message text bhejo:{hint}",
                reply_markup=admin_edit_msg_kb(key),
            )
            await query.answer()

    elif data.startswith("admin_reset_msg_"):
        key = data[len("admin_reset_msg_"):]
        if key not in DEFAULT_MESSAGES:
            await query.answer("❓ Unknown message", show_alert=True)
        else:
            _bot_messages[key] = DEFAULT_MESSAGES[key]
            await set_setting(f"msg_{key}", DEFAULT_MESSAGES[key])
            _admin_state["awaiting"] = None
            await query.answer(f"🔄 {MESSAGE_LABELS[key]} reset ho gaya.", show_alert=True)
            await query.message.edit_text("📝 Kaunsa message edit karna hai?", reply_markup=admin_messages_kb())

    elif data == "admin_reset_all_msgs":
        for key, default_text in DEFAULT_MESSAGES.items():
            _bot_messages[key] = default_text
            await set_setting(f"msg_{key}", default_text)
        _admin_state["awaiting"] = None
        await query.answer("🔄 Sabhi messages default pe reset ho gaye.", show_alert=True)
        await query.message.edit_text("📝 Kaunsa message edit karna hai?", reply_markup=admin_messages_kb())

    elif data == "admin_back_main":
        _admin_state["awaiting"] = None
        await query.message.edit_text("🛠 Admin Panel:", reply_markup=admin_main_kb())
        await query.answer()

    elif data == "admin_toggle_fsub":
        _bot_settings["fsub_enabled"] = not _bot_settings["fsub_enabled"]
        await set_setting("fsub_enabled", "1" if _bot_settings["fsub_enabled"] else "0")
        await query.message.edit_text("⚙️ Settings:", reply_markup=admin_settings_kb())
        state = "🔔 ON" if _bot_settings["fsub_enabled"] else "🔕 OFF"
        await query.answer(f"Force Sub ab {state} hai", show_alert=True)

    elif data == "admin_set_fsub_id":
        _admin_state["awaiting"] = "fsub_channel_id"
        await query.message.edit_text(
            f"🆔 Naya FSUB channel ID/username bhejo (jaise -1001234567890 ya @channelusername).\nAbhi: {_bot_settings['fsub_channel_id']}",
            reply_markup=admin_cancel_kb(),
        )
        await query.answer()

    elif data == "admin_set_limit":
        _admin_state["awaiting"] = "upload_limit"
        await query.message.edit_text(
            f"📦 Naya upload limit MB mein bhejo (number only).\nAbhi: {_bot_settings['upload_limit_mb']}MB",
            reply_markup=admin_cancel_kb(),
        )
        await query.answer()

    elif data == "admin_set_fsub_link":
        _admin_state["awaiting"] = "fsub_link"
        await query.message.edit_text(
            f"🔗 Naya FSUB channel link bhejo (https:// se shuru).\nAbhi: {_bot_settings['fsub_channel_link']}",
            reply_markup=admin_cancel_kb(),
        )
        await query.answer()

    elif data == "admin_cleanup":
        before = len(os.listdir(DOWNLOAD_DIR)) if os.path.isdir(DOWNLOAD_DIR) else 0
        cleanup_old_files(DOWNLOAD_DIR, days=3)
        after = len(os.listdir(DOWNLOAD_DIR)) if os.path.isdir(DOWNLOAD_DIR) else 0
        await query.answer(f"🧹 Cleanup done. {before - after} purani file(s) hati.", show_alert=True)

    elif data == "admin_quit":
        _admin_state["awaiting"] = None
        await query.message.edit_text("✅ Admin panel band ho gaya. /admin se dobara khol sakta hai.")
        await query.answer()

    else:
        await query.answer("❓ Unknown action", show_alert=True)

async def admin_input_receive(client: Client, message: Message):
    awaiting = _admin_state.get("awaiting")
    text = message.text
    if not text:
        await message.reply_text("⚠️ Text bhejo bhai.")
        return

    if awaiting == "broadcast":
        _admin_state["awaiting"] = None
        await message.reply_text("📢 Broadcast shuru ho raha hai...")
        sent = 0
        for uid in await get_all_user_ids():
            try:
                await asyncio.sleep(0.05)
                await client.send_message(uid, text)
                sent += 1
            except FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception:
                pass
        await message.reply_text(f"✅ Broadcast khatam. {sent} users ko bheja gaya.")

    elif awaiting == "upload_limit":
        _admin_state["awaiting"] = None
        try:
            new_limit = int(text.strip())
            if new_limit <= 0:
                raise ValueError
            _bot_settings["upload_limit_mb"] = new_limit
            await set_setting("upload_limit_mb", new_limit)
            await message.reply_text(f"✅ Upload limit ab {new_limit}MB hai.")
        except ValueError:
            await message.reply_text("⚠️ Sirf positive number bhejo (MB mein). /admin se phir try karo.")

    elif awaiting == "fsub_link":
        _admin_state["awaiting"] = None
        new_link = text.strip()
        if not (new_link.startswith("http://") or new_link.startswith("https://")):
            await message.reply_text("⚠️ Valid link nahi laga, https:// se shuru hona chahiye. /admin se phir try karo.")
        else:
            _bot_settings["fsub_channel_link"] = new_link
            await set_setting("fsub_channel_link", new_link)
            await message.reply_text(f"✅ FSUB link update ho gaya:\n🔗 {new_link}")

    elif awaiting == "fsub_channel_id":
        _admin_state["awaiting"] = None
        new_id = text.strip()
        valid = new_id.startswith("@") and len(new_id) > 1
        if not valid:
            try:
                int(new_id)
                valid = True
            except ValueError:
                valid = False
        if not valid:
            await message.reply_text("⚠️ Valid channel ID nahi laga. Numeric ID (jaise -1001234567890) ya @username bhejo. /admin se phir try karo.")
        else:
            _bot_settings["fsub_channel_id"] = new_id
            await set_setting("fsub_channel_id", new_id)
            await message.reply_text(f"✅ FSUB Channel ID update ho gaya:\n🆔 {new_id}")

    elif awaiting and awaiting.startswith("msg_"):
        _admin_state["awaiting"] = None
        key = awaiting[len("msg_"):]
        if key not in DEFAULT_MESSAGES:
            await message.reply_text("⚠️ Kuch gadbad ho gayi. /admin se phir try karo.")
        else:
            _bot_messages[key] = text
            await set_setting(f"msg_{key}", text)
            label = MESSAGE_LABELS[key]
            note = ""
            if "{name}" in DEFAULT_MESSAGES[key] and "{name}" not in text:
                note = "\n\n💡 Note: is message mein `{name}` nahi hai, isliye user ka naam show nahi hoga."
            await message.reply_text(f"✅ {label} update ho gaya!\n\nNaya message:\n{text}{note}")

    else:
        _admin_state["awaiting"] = None

# main message handler for links
def check_rate_limit(user_id: int) -> bool:
    """Sliding-window rate limit: last RATELIMIT_WINDOW seconds me
    RATELIMIT_COUNT ya usse zyada download-requests ki to True (blocked)."""
    now = time.time()
    history = download_times.get(user_id, [])
    history = [t for t in history if now - t < RATELIMIT_WINDOW]
    download_times[user_id] = history
    return len(history) >= RATELIMIT_COUNT


def record_download_attempt(user_id: int):
    download_times.setdefault(user_id, []).append(time.time())


async def download_handler(client: Client, message: Message):
    text = message.text.strip()
    urls = URL_RE.findall(text)
    if not urls:
        return
    url = None
    for u in urls:
        if (
            INST_RE.search(u) or X_RE.search(u) or YT_RE.search(u)
            or TT_RE.search(u) or PIN_RE.search(u) or REDDIT_RE.search(u) or FB_RE.search(u)
        ):
            url = u
            break
    if not url:
        await message.reply_text("⚠️")
        return

    if check_rate_limit(message.from_user.id):
        await message.reply_text(f"🤖 Wait {RATELIMIT_WAIT}s....")
        return

    if not await check_fsub(client, message.from_user.id):
        await message.reply_text("🗝️", reply_markup=fsub_join_kb())
        return

    record_download_attempt(message.from_user.id)
    await add_user(message.from_user.id, message.from_user.first_name or "User")

    status_msg = await message.reply_text("🪪", quote=True)

    # Telegram bot API ka hard upload limit ~2GB hai, chahe video ho ya document.
    # Pehle metadata se size estimate le lete hain — agar clearly 2GB se zyada
    # hai to poori file download hi nahi karte (bandwidth/time/disk bachta hai).
    HARD_LIMIT_BYTES = 2 * 1024 * 1024 * 1024
    estimated_size = await estimate_filesize(url)
    if estimated_size and estimated_size > HARD_LIMIT_BYTES:
        await _safe_edit(status_msg, "🚫")
        return

    filenames: list = []
    try:
        filenames = await download_media(url, DOWNLOAD_DIR, message.from_user.id)
        if not filenames:
            await _safe_edit(status_msg, "❌ Is link se kuch download nahi ho paya (sab engines fail ho gaye).")
            return

        await _safe_edit(status_msg, "📤")
        limit_bytes = _bot_settings["upload_limit_mb"] * 1024 * 1024

        for fname in filenames:
            try:
                filesize = os.path.getsize(fname)
                ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
                is_photo = ext in ("jpg", "jpeg", "png", "webp")
                is_video = ext in ("mp4", "mkv", "mov", "webm", "avi")

                if filesize > limit_bytes:
                    await message.reply_document(document=fname, quote=True)
                elif is_photo:
                    await message.reply_photo(photo=fname, quote=True)
                elif is_video:
                    try:
                        await message.reply_video(video=fname, quote=True)
                    except Exception:
                        await message.reply_document(document=fname, quote=True)
                else:
                    await message.reply_document(document=fname, quote=True)
            except Exception:
                logger.exception(f"File bhejne me fail hua: {fname}")

        await status_msg.delete()
        await add_download(message.from_user.id)

    except Exception as e:
        logger.exception("Download/upload failed")
        try:
            await status_msg.edit_text("❌")
        except Exception:
            pass
    finally:
        for fname in filenames:
            try:
                if os.path.exists(fname):
                    os.remove(fname)
                parent = os.path.dirname(fname)
                # instaloader/gallery-dl temp sub-folders khaali hone par saaf kar do
                if parent and parent != os.path.normpath(DOWNLOAD_DIR) and os.path.isdir(parent) and not os.listdir(parent):
                    os.rmdir(parent)
            except Exception:
                pass

async def _safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text)
    except Exception:
        pass

# ----------------- Scheduler tasks -----------------
# scheduler is created inside _run(), same reason as the Client — see note above.

async def send_good_morning():
    text = render_msg("gm_msg")
    for uid in await get_all_user_ids():
        try:
            await app.send_message(uid, text)
            await asyncio.sleep(0.1)
        except Exception:
            pass

def schedule_jobs(scheduler: AsyncIOScheduler):
    # cleanup at 03:05 daily
    scheduler.add_job(lambda: cleanup_old_files(DOWNLOAD_DIR, days=3), CronTrigger(hour=3, minute=5))
    # morning message at 06:00 — AsyncIOScheduler coroutine functions ko seedha
    # schedule kar sakta hai; asyncio.create_task() ka manual wrapping crash
    # karta tha kyunki scheduler ka job apne thread me chalta hai jaha koi
    # running event loop nahi hota.
    scheduler.add_job(send_good_morning, CronTrigger(hour=6, minute=0))

# ----------------- Run -----------------

app: Optional[Client] = None

async def _run():
    global app
    app = Client("KissuDLBot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

    app.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    app.add_handler(MessageHandler(help_handler, filters.regex("^Help$") & filters.private))
    app.add_handler(MessageHandler(account_handler, filters.regex("^Account$") & filters.private))
    app.add_handler(MessageHandler(about_handler, filters.regex("^About$") & filters.private))
    app.add_handler(MessageHandler(admin_handler, filters.command("admin") & filters.private))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(
        admin_input_receive,
        filters.private & filters.user(ADMIN_ID) & awaiting_admin_input_filter & not_command_filter,
    ))
    app.add_handler(MessageHandler(download_handler, filters.text & filters.private & not_command_filter))

    scheduler = AsyncIOScheduler(timezone=pytz.timezone(TIMEZONE))
    schedule_jobs(scheduler)
    scheduler.start()

    await app.start()
    logger.info("Bot started")
    await idle()
    await app.stop()

if __name__ == "__main__":
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down")
