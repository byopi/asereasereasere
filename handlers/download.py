"""
handlers/download.py — Lógica de transferencia estable vía Pipes (0-RAM).
Soluciona el error 'Invalid file' y permite archivos de hasta 2GB en Render Free.
"""

import asyncio
import logging
import os
from typing import Optional

from pyrogram import Client
from pyrogram.errors import (
    FloodWait,
    MessageIdInvalid,
    ChannelPrivate,
    ChatForwardsRestricted,
    MessageNotModified,
    PeerIdInvalid,
)
from pyrogram.types import Message

from config import CHUNK_SIZE, MAX_BATCH_SIZE, PROGRESS_UPDATE_INTERVAL
from utils.parser import parse_telegram_url
from utils.progress import ProgressTracker

logger = logging.getLogger(__name__)

async def handle_dl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply_text("❌ `/dl <url_del_mensaje>`")
        return
    parsed = parse_telegram_url(args[1].strip())
    if not parsed:
        await message.reply_text("❌ URL inválida.")
        return
    chat_id, msg_id = parsed
    status = await message.reply_text("🔍 Obteniendo mensaje...")
    await _process_single(bot, user, message, status, chat_id, msg_id)

async def handle_bdl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text("❌ `/bdl <url_inicio> <url_fin>`")
        return
    p1, p2 = parse_telegram_url(args[1]), parse_telegram_url(args[2])
    if not p1 or not p2 or p1[0] != p2[0]:
        await message.reply_text("❌ URLs inválidas o de distintos canales.")
        return
    start_id, end_id = (p1[1], p2[1]) if p1[1] <= p2[1] else (p2[1], p1[1])
    total = end_id - start_id + 1
    if total > MAX_BATCH_SIZE:
        await message.reply_text(f"❌ Máximo {MAX_BATCH_SIZE} mensajes.")
        return
    status = await message.reply_text(f"📦 Lote: `{start_id}` → `{end_id}`")
    ok, fail = 0, 0
    for i, msg_id in enumerate(range(start_id, end_id + 1), 1):
        try:
            await _process_single(bot, user, message, status, p1[0], msg_id, f"[{i}/{total}]")
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(1.5)
    await status.edit_text(f"✅ Éxitos: `{ok}` | ❌ Fallos: `{fail}`")

async def _process_single(bot: Client, user: Client, original_msg: Message, status_msg: Message, chat_id: str, msg_id: int, pfx: str = "") -> None:
    pfx = f"{pfx} " if pfx else ""
    try:
        src = await _get_message_with_retry(user, chat_id, msg_id)
    except Exception as e:
        await _safe_edit(status_msg, f"{pfx}❌ Error: {e}")
        return
    if not src:
        await _safe_edit(status_msg, f"{pfx}⚠️ No existe.")
        return
    if not src.media:
        await bot.send_message(original_msg.chat.id, src.text or src.caption or "...")
        await _safe_edit(status_msg, f"{pfx}✅ Texto copiado.")
        return
    try:
        await user.copy_message(original_msg.chat.id, chat_id, msg_id)
        await _safe_edit(status_msg, f"{pfx}✅ Copia directa.")
    except Exception:
        await _safe_edit(status_msg, f"{pfx}⚡ Canal restringido. Usando Pipe...")
        await _stream_and_send(bot, user, original_msg, status_msg, src, pfx)

async def _stream_and_send(bot: Client, user: Client, original_msg: Message, status_msg: Message, src: Message, pfx: str) -> None:
    total_size = _get_media_size(src)
    file_name = _get_media_name(src) or f"file_{src.id}"
    tracker = ProgressTracker(total_size, status_msg, pfx, PROGRESS_UPDATE_INTERVAL)
    
    r, w = os.pipe()
    reader = os.fdopen(r, "rb")
    writer = os.fdopen(w, "wb")
    setattr(reader, "name", file_name)

    async def download():
        try:
            async for chunk in user.stream_media(src):
                if chunk:
                    writer.write(chunk)
                    await tracker.update(len(chunk))
            writer.flush()
        finally:
            writer.close()

    async def upload():
        try:
            await _dispatch_media(bot, original_msg.chat.id, src, reader, src.caption or "")
        finally:
            reader.close()

    try:
        await asyncio.gather(download(), upload())
        await _safe_edit(status_msg, f"{pfx}✅ `{file_name}` enviado.")
    except Exception as e:
        await _safe_edit(status_msg, f"{pfx}❌ Error: `{e}`")

async def _dispatch_media(bot: Client, chat_id: int, src: Message, fp, caption: str) -> None:
    kw = dict(chat_id=chat_id, caption=caption)
    m = src.media.value if src.media else "document"
    if m == "video": await bot.send_video(video=fp, **kw)
    elif m == "audio": await bot.send_audio(audio=fp, **kw)
    elif m == "voice": await bot.send_voice(voice=fp, **kw)
    elif m == "photo": await bot.send_photo(photo=fp, **kw)
    elif m == "animation": await bot.send_animation(animation=fp, **kw)
    else: await bot.send_document(document=fp, **kw)

async def _get_message_with_retry(user: Client, chat_id: str, msg_id: int) -> Optional[Message]:
    raw_id = str(chat_id).replace("c/", "")
    target_id = int(f"-100{raw_id}") if raw_id.isdigit() else chat_id
    for _ in range(3):
        try: return await user.get_messages(target_id, msg_id)
        except FloodWait as e: await asyncio.sleep(e.value + 1)
        except Exception: await asyncio.sleep(2)
    return None

async def _safe_edit(msg: Message, text: str) -> None:
    try: await msg.edit_text(text)
    except Exception: pass

def _get_media_size(msg: Message) -> int:
    for a in ("video", "audio", "document", "voice", "animation"):
        obj = getattr(msg, a, None)
        if obj and hasattr(obj, "file_size"): return obj.file_size
    return 0

def _get_media_name(msg: Message) -> Optional[str]:
    for a in ("video", "audio", "document", "animation"):
        obj = getattr(msg, a, None)
        if obj and getattr(obj, "file_name", None): return obj.file_name
    return None
