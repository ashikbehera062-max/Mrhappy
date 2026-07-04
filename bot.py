"""
bot.py
------
Production-style File-to-Link / Private Storage Bot using Pyrogram.

Features:
  - Admin-only upload (regular users' media is deleted/ignored)
  - Force Subscribe gate before any bot usage
  - Custom caption saving per file
  - Batch upload -> single link for multiple files
  - Password-protected links
  - Download analytics / /stats command
  - ZIP download for batches
  - Broadcast to all known users
  - Inline control buttons for admin (Download / Revoke / Close)

Run:
    python bot.py
"""

import os
import io
import time
import asyncio
import logging
import zipfile
import tempfile
from typing import Dict, List

from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from pyrogram.errors import (
    UserNotParticipant,
    FloodWait,
    UserIsBlocked,
    InputUserDeactivated,
    RPCError,
)

from database import db

# ---------------------------------------------------------------------------
# Setup & Config
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("bot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
FSUB_CHANNEL_ID = int(os.environ["FSUB_CHANNEL_ID"])
LOG_CHANNEL_ID = os.environ.get("LOG_CHANNEL_ID") or None
if LOG_CHANNEL_ID:
    LOG_CHANNEL_ID = int(LOG_CHANNEL_ID)
BOT_USERNAME = os.environ.get("BOT_USERNAME", "")

app = Client(
    "storage_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

# In-memory batch buffer: admin_id -> {"link_id":.., "files":[...], "task": asyncio.Task}
PENDING_BATCHES: Dict[int, Dict] = {}
BATCH_TIMEOUT = 6  # seconds of inactivity before a batch auto-finalizes

# In-memory state for users typing a password: user_id -> link_id waiting for
AWAITING_PASSWORD: Dict[int, str] = {}

FILE_TYPE_FILTER = filters.photo | filters.video | filters.audio | filters.document


# ---------------------------------------------------------------------------
# Force-Subscribe helper
# ---------------------------------------------------------------------------

async def is_subscribed(client: Client, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(FSUB_CHANNEL_ID, user_id)
        if member.status in (
            enums.ChatMemberStatus.BANNED,
        ):
            return False
        return True
    except UserNotParticipant:
        return False
    except RPCError as e:
        logger.warning(f"FSub check error: {e}")
        return False


async def fsub_markup(client: Client) -> InlineKeyboardMarkup:
    try:
        invite_link = await client.export_chat_invite_link(FSUB_CHANNEL_ID)
    except RPCError:
        chat = await client.get_chat(FSUB_CHANNEL_ID)
        invite_link = chat.invite_link or "https://t.me"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📢 Join Channel", url=invite_link)],
            [InlineKeyboardButton("🔄 Try Again", callback_data="check_fsub")],
        ]
    )


async def send_fsub_prompt(client: Client, message_or_query, pending_payload: str = ""):
    text = (
        "🔒 **Access Restricted**\n\n"
        "You must join our channel to use this bot / access this file.\n"
        "Tap **Join Channel**, then **Try Again**."
    )
    markup = await fsub_markup(client)
    if isinstance(message_or_query, CallbackQuery):
        await message_or_query.message.edit_text(text, reply_markup=markup)
    else:
        await message_or_query.reply_text(text, reply_markup=markup)


# ---------------------------------------------------------------------------
# /start  (also handles deep links: /start <link_id>)
# ---------------------------------------------------------------------------

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    await db.add_user(user_id, message.from_user.username)

    subscribed = await is_subscribed(client, user_id)
    if not subscribed:
        await send_fsub_prompt(client, message)
        return

    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        link_id = args[1].strip()
        await deliver_link(client, message, link_id)
        return

    await message.reply_text(
        "👋 **Welcome!**\n\n"
        "This is a private file storage bot.\n"
        "Use a shared link to access files.",
    )


@app.on_callback_query(filters.regex("^check_fsub$"))
async def check_fsub_callback(client: Client, query: CallbackQuery):
    user_id = query.from_user.id
    subscribed = await is_subscribed(client, user_id)
    if subscribed:
        await query.message.edit_text("✅ Access granted! Send /start again or reopen your link.")
    else:
        await query.answer("You still haven't joined the channel.", show_alert=True)


# ---------------------------------------------------------------------------
# Link delivery (with password gate + click tracking)
# ---------------------------------------------------------------------------

async def deliver_link(client: Client, message: Message, link_id: str):
    link = await db.get_link(link_id)
    if not link or link.get("revoked"):
        await message.reply_text("❌ This link is invalid or has been revoked.")
        return

    user_id = message.from_user.id

    if link.get("password"):
        AWAITING_PASSWORD[user_id] = link_id
        await message.reply_text(
            "🔑 This content is **password protected**.\n"
            "Please send the password to continue."
        )
        return

    await send_link_content(client, message.chat.id, link_id, link)


async def send_link_content(client: Client, chat_id: int, link_id: str, link: dict):
    files = await db.get_files_for_link(link_id)
    if not files:
        await client.send_message(chat_id, "❌ No files found for this link.")
        return

    await db.increment_click(link_id)

    caption_text = link.get("caption") or ""

    # Send files one by one, preserving type
    for f in files:
        try:
            file_type = f["file_type"]
            send_kwargs = {"caption": f.get("caption") or caption_text}
            if file_type == "photo":
                await client.send_photo(chat_id, f["file_id"], **send_kwargs)
            elif file_type == "video":
                await client.send_video(chat_id, f["file_id"], **send_kwargs)
            elif file_type == "audio":
                await client.send_audio(chat_id, f["file_id"], **send_kwargs)
            else:
                await client.send_document(chat_id, f["file_id"], **send_kwargs)
            await asyncio.sleep(0.5)  # gentle pacing to avoid flood limits
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
        except RPCError as e:
            logger.warning(f"Error sending file for link {link_id}: {e}")

    if len(files) > 1:
        markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📦 Download as ZIP", callback_data=f"zip_{link_id}")]]
        )
        await client.send_message(chat_id, "All files sent above. Want them bundled?", reply_markup=markup)


@app.on_message(filters.private & filters.text & ~filters.command(["start", "stats", "broadcast", "set_password", "remove_password", "batch", "endbatch", "revoke"]))
async def password_text_handler(client: Client, message: Message):
    """Catches plain text messages that might be password attempts."""
    user_id = message.from_user.id
    if user_id not in AWAITING_PASSWORD:
        return  # not in a password flow; ignore silently

    link_id = AWAITING_PASSWORD[user_id]
    link = await db.get_link(link_id)
    if not link or link.get("revoked"):
        AWAITING_PASSWORD.pop(user_id, None)
        await message.reply_text("❌ This link is no longer valid.")
        return

    if message.text.strip() == link.get("password"):
        AWAITING_PASSWORD.pop(user_id, None)
        await message.reply_text("✅ Correct password! Sending your files...")
        await send_link_content(client, message.chat.id, link_id, link)
    else:
        await message.reply_text("❌ Incorrect password. Try again.")


# ---------------------------------------------------------------------------
# ZIP creation callback
# ---------------------------------------------------------------------------

@app.on_callback_query(filters.regex(r"^zip_(.+)$"))
async def zip_callback(client: Client, query: CallbackQuery):
    link_id = query.data.split("_", 1)[1]
    await query.answer("Preparing ZIP... this may take a moment.", show_alert=False)

    files = await db.get_files_for_link(link_id)
    if not files:
        await query.message.reply_text("❌ No files found to zip.")
        return

    status_msg = await query.message.reply_text("📦 Downloading files and building ZIP...")

    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = os.path.join(tmp_dir, f"{link_id}.zip")
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for idx, f in enumerate(files, start=1):
                    try:
                        local_path = await client.download_media(
                            f["file_id"], file_name=os.path.join(tmp_dir, f"file_{idx}")
                        )
                        if local_path:
                            arcname = f.get("file_name") or os.path.basename(local_path)
                            zf.write(local_path, arcname=arcname)
                    except RPCError as e:
                        logger.warning(f"ZIP download error: {e}")

            await status_msg.edit_text("📤 Uploading ZIP...")
            await client.send_document(
                query.message.chat.id,
                zip_path,
                caption=f"📦 Bundle for link `{link_id}`",
            )
            await status_msg.delete()
        except Exception as e:
            logger.error(f"ZIP creation failed: {e}")
            await status_msg.edit_text(f"❌ Failed to create ZIP: {e}")


# ---------------------------------------------------------------------------
# ADMIN: File Upload Handling (single + batch)
# ---------------------------------------------------------------------------

def extract_file_meta(message: Message):
    """Return (file_id, file_unique_id, file_type, file_name, file_size)."""
    if message.photo:
        m = message.photo
        return m.file_id, m.file_unique_id, "photo", None, m.file_size
    if message.video:
        m = message.video
        return m.file_id, m.file_unique_id, "video", m.file_name, m.file_size
    if message.audio:
        m = message.audio
        return m.file_id, m.file_unique_id, "audio", m.file_name, m.file_size
    if message.document:
        m = message.document
        return m.file_id, m.file_unique_id, "document", m.file_name, m.file_size
    return None


@app.on_message(FILE_TYPE_FILTER & filters.private)
async def upload_handler(client: Client, message: Message):
    user_id = message.from_user.id if message.from_user else None

    # Non-admins: silently delete/ignore
    if user_id != ADMIN_ID:
        try:
            await message.delete()
        except RPCError:
            pass
        return

    meta = extract_file_meta(message)
    if not meta:
        return
    file_id, file_unique_id, file_type, file_name, file_size = meta
    caption = message.caption or None

    # Optionally back up to a log/storage channel for persistence
    if LOG_CHANNEL_ID:
        try:
            await message.copy(LOG_CHANNEL_ID)
        except RPCError as e:
            logger.warning(f"Could not copy to log channel: {e}")

    if user_id in PENDING_BATCHES:
        batch = PENDING_BATCHES[user_id]
        batch["files"].append(
            {
                "file_id": file_id,
                "file_unique_id": file_unique_id,
                "file_type": file_type,
                "file_name": file_name,
                "file_size": file_size,
                "caption": caption,
            }
        )
        # reset the auto-finalize timer
        if batch["task"]:
            batch["task"].cancel()
        batch["task"] = asyncio.create_task(auto_finalize_batch(client, user_id))
        await message.reply_text(
            f"➕ Added to batch (`{batch['link_id']}`). Total files: {len(batch['files'])}\n"
            f"Send more, or use /endbatch to finish now."
        )
        return

    # Single file -> generate link immediately
    link_id = db.generate_id()
    await db.create_link(link_id, admin_id=user_id, caption=caption, is_batch=False)
    await db.add_file(link_id, file_id, file_unique_id, file_type, caption, file_name, file_size)
    await send_admin_link_summary(client, message, link_id, count=1)


async def auto_finalize_batch(client: Client, user_id: int):
    try:
        await asyncio.sleep(BATCH_TIMEOUT)
    except asyncio.CancelledError:
        return
    await finalize_batch(client, user_id)


async def finalize_batch(client: Client, user_id: int):
    batch = PENDING_BATCHES.pop(user_id, None)
    if not batch:
        return
    link_id = batch["link_id"]
    files = batch["files"]
    if not files:
        return
    for f in files:
        await db.add_file(
            link_id,
            f["file_id"],
            f["file_unique_id"],
            f["file_type"],
            f["caption"],
            f["file_name"],
            f["file_size"],
        )
    await client.send_message(
        user_id,
        f"✅ Batch finalized with **{len(files)}** file(s).",
    )
    await send_admin_link_summary(client, await client.get_messages(user_id, user_id), link_id, count=len(files), is_batch=True)


@app.on_message(filters.command("batch") & filters.user(ADMIN_ID))
async def batch_start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id in PENDING_BATCHES:
        await message.reply_text("⚠️ A batch is already in progress. Use /endbatch to finish it first.")
        return
    link_id = db.generate_id()
    await db.create_link(link_id, admin_id=user_id, is_batch=True)
    PENDING_BATCHES[user_id] = {"link_id": link_id, "files": [], "task": None}
    await message.reply_text(
        f"📥 Batch mode started (`{link_id}`).\n"
        f"Send all files now. I'll auto-finish after {BATCH_TIMEOUT}s of inactivity, "
        f"or use /endbatch to finish immediately."
    )


@app.on_message(filters.command("endbatch") & filters.user(ADMIN_ID))
async def batch_end_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in PENDING_BATCHES:
        await message.reply_text("⚠️ No active batch to end.")
        return
    batch = PENDING_BATCHES[user_id]
    if batch["task"]:
        batch["task"].cancel()
    await finalize_batch(client, user_id)


async def send_admin_link_summary(client: Client, message: Message, link_id: str, count: int, is_batch: bool = False):
    deep_link = f"https://t.me/{BOT_USERNAME}?start={link_id}" if BOT_USERNAME else f"(set BOT_USERNAME in .env) start={link_id}"
    text = (
        f"✅ **Link Generated**\n\n"
        f"🆔 Link ID: `{link_id}`\n"
        f"📁 Files: {count}\n"
        f"🗂 Type: {'Batch' if is_batch else 'Single'}\n\n"
        f"🔗 {deep_link}\n\n"
        f"To password-protect: `/set_password {link_id} yourpassword`"
    )
    markup = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬇️ Download", url=deep_link if BOT_USERNAME else "https://t.me"),
                InlineKeyboardButton("🗑 Revoke", callback_data=f"revoke_{link_id}"),
            ],
            [InlineKeyboardButton("❌ Close", callback_data="close")],
        ]
    )
    await message.reply_text(text, reply_markup=markup, disable_web_page_preview=True)


@app.on_callback_query(filters.regex(r"^revoke_(.+)$"))
async def revoke_callback(client: Client, query: CallbackQuery):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Not authorized.", show_alert=True)
        return
    link_id = query.data.split("_", 1)[1]
    await db.revoke_link(link_id)
    await query.message.edit_text(f"🗑 Link `{link_id}` has been revoked.")


@app.on_callback_query(filters.regex("^close$"))
async def close_callback(client: Client, query: CallbackQuery):
    try:
        await query.message.delete()
    except RPCError:
        pass


# ---------------------------------------------------------------------------
# ADMIN: /set_password  and /remove_password
# ---------------------------------------------------------------------------

@app.on_message(filters.command("set_password") & filters.user(ADMIN_ID))
async def set_password_handler(client: Client, message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply_text("Usage: `/set_password <link_id> <password>`")
        return
    _, link_id, password = parts
    link = await db.get_link(link_id)
    if not link:
        await message.reply_text("❌ Link ID not found.")
        return
    await db.set_password(link_id, password)
    await message.reply_text(f"🔑 Password set for link `{link_id}`.")


@app.on_message(filters.command("remove_password") & filters.user(ADMIN_ID))
async def remove_password_handler(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/remove_password <link_id>`")
        return
    link_id = parts[1].strip()
    link = await db.get_link(link_id)
    if not link:
        await message.reply_text("❌ Link ID not found.")
        return
    await db.remove_password(link_id)
    await message.reply_text(f"🔓 Password removed for link `{link_id}`.")


@app.on_message(filters.command("revoke") & filters.user(ADMIN_ID))
async def revoke_command_handler(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply_text("Usage: `/revoke <link_id>`")
        return
    link_id = parts[1].strip()
    ok = await db.revoke_link(link_id)
    if ok:
        await message.reply_text(f"🗑 Link `{link_id}` revoked.")
    else:
        await message.reply_text("❌ Link ID not found.")


# ---------------------------------------------------------------------------
# ADMIN: /stats
# ---------------------------------------------------------------------------

@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_handler(client: Client, message: Message):
    total_users = await db.total_users_count()
    total_links = await db.total_links_count()
    top = await db.top_links(limit=10)

    lines = [
        "📊 **Bot Statistics**\n",
        f"👥 Total Users: `{total_users}`",
        f"🔗 Total Links: `{total_links}`\n",
        "🏆 **Top Downloaded Links:**",
    ]
    if not top:
        lines.append("_No downloads yet._")
    else:
        for i, link in enumerate(top, start=1):
            lines.append(f"{i}. `{link['link_id']}` — {link.get('clicks', 0)} clicks")

    await message.reply_text("\n".join(lines))


# ---------------------------------------------------------------------------
# ADMIN: /broadcast
# ---------------------------------------------------------------------------

@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast_handler(client: Client, message: Message):
    if not message.reply_to_message:
        await message.reply_text(
            "⚠️ Reply to the message (text/photo/video/etc.) you want to broadcast with `/broadcast`."
        )
        return

    source = message.reply_to_message
    users = await db.get_all_users()
    total = len(users)
    status = await message.reply_text(f"📢 Broadcasting to {total} users...")

    sent, failed, blocked = 0, 0, 0

    for user_id in users:
        try:
            await source.copy(user_id)
            sent += 1
        except FloodWait as fw:
            await asyncio.sleep(fw.value)
            try:
                await source.copy(user_id)
                sent += 1
            except RPCError:
                failed += 1
        except (UserIsBlocked, InputUserDeactivated):
            blocked += 1
            await db.remove_user(user_id)
        except RPCError as e:
            logger.warning(f"Broadcast error for {user_id}: {e}")
            failed += 1
        await asyncio.sleep(0.05)  # small pacing to respect rate limits

    await status.edit_text(
        f"✅ **Broadcast Complete**\n\n"
        f"👥 Total: {total}\n"
        f"✅ Sent: {sent}\n"
        f"🚫 Blocked/Deactivated: {blocked}\n"
        f"❌ Failed: {failed}"
    )


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

async def main():
    await db.ensure_indexes()
    await app.start()
    me = await app.get_me()
    logger.info(f"Bot started as @{me.username}")
    if not BOT_USERNAME:
        logger.warning(
            "BOT_USERNAME is not set in .env — deep links in admin summaries will be incomplete. "
            f"Set BOT_USERNAME={me.username} in your .env and restart."
        )
    await asyncio.Event().wait()  # keep alive


if __name__ == "__main__":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass
    asyncio.run(main())