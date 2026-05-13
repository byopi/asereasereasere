"""
handlers/download.py — Lógica de transferencia con Piping (0-RAM).

Gestión de memoria (Streaming Directo):
─────────────────────────────────────────────────────────────
  En lugar de acumular el archivo en RAM, usamos un generador asíncrono.
  Pyrogram descarga un chunk y lo envía inmediatamente al método de subida.

  ┌─────────────────────────────────────────────────────────────┐
  │  RAM usada ≈ 0 (solo el chunk actual en tránsito)           │
  │  Disco usado = 0 bytes                                      │
  └─────────────────────────────────────────────────────────────┘

  • Funciona en el plan FREE de Render para cualquier tamaño (< 2 GB).
  • Se evita el error OOM (Out of Memory).
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional

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


# ══════════════════════════════════════════════════════════════════════════════
# /dl — Descarga un solo mensaje
# ══════════════════════════════════════════════════════════════════════════════

async def handle_dl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply_text(
            "❌ **Uso incorrecto**\n`/dl <url_del_mensaje>`\n\n"
            "Ejemplo: `/dl https://t.me/canal/123`"
        )
        return

    parsed = parse_telegram_url(args[1].strip())
    if not parsed:
        await message.reply_text(
            "❌ **URL inválida.**\n"
            "Formatos aceptados:\n"
            "• `https://t.me/canal/123`\n"
            "• `https://t.me/c/1234567890/123`"
        )
        return

    chat_id, msg_id = parsed
    status = await message.reply_text("🔍 Obteniendo mensaje...")
    await _process_single(bot, user, message, status, chat_id, msg_id)


# ══════════════════════════════════════════════════════════════════════════════
# /bdl — Descarga por lotes
# ══════════════════════════════════════════════════════════════════════════════

async def handle_bdl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text(
            "❌ **Uso incorrecto**\n`/bdl <url_inicio> <url_fin>`\n\n"
            "Ejemplo: `/bdl https://t.me/canal/100 https://t.me/canal/110`"
        )
        return

    p1 = parse_telegram_url(args[1])
    p2 = parse_telegram_url(args[2])

    if not p1 or not p2:
        await message.reply_text("❌ Una o ambas URLs son inválidas.")
        return

    chat1, start_id = p1
    chat2, end_id = p2

    if chat1 != chat2:
        await message.reply_text("❌ Ambas URLs deben ser del mismo canal.")
        return

    if start_id > end_id:
        start_id, end_id = end_id, start_id

    total = end_id - start_id + 1
    if total > MAX_BATCH_SIZE:
        await message.reply_text(
            f"❌ El lote tiene {total} mensajes. Máximo permitido: {MAX_BATCH_SIZE}.\n"
            f"Divide el rango en partes más pequeñas."
        )
        return

    status = await message.reply_text(
        f"📦 **Lote iniciado**\n"
        f"Canal: `{chat1}`\n"
        f"Mensajes: `{start_id}` → `{end_id}` ({total} total)"
    )

    ok, fail = 0, 0
    for i, msg_id in enumerate(range(start_id, end_id + 1), 1):
        batch_prefix = f"[{i}/{total}]"
        try:
            await _process_single(
                bot, user, message, status, chat1, msg_id,
                batch_prefix=batch_prefix,
            )
            ok += 1
        except Exception as e:
            fail += 1
            logger.warning("%s Error en msg %d: %s", batch_prefix, msg_id, e)

        await asyncio.sleep(1.5)

    await status.edit_text(
        f"{'✅' if fail == 0 else '⚠️'} **Lote completado**\n"
        f"✅ Éxitos: `{ok}` | ❌ Fallos: `{fail}` | Total: `{total}`"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Núcleo: procesar un solo mensaje con fallback
# ══════════════════════════════════════════════════════════════════════════════

async def _process_single(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    chat_id: str,
    msg_id: int,
    batch_prefix: str = "",
) -> None:
    pfx = f"{batch_prefix} " if batch_prefix else ""

    try:
        src = await _get_message_with_retry(user, chat_id, msg_id)
    except (MessageIdInvalid, ChannelPrivate, PeerIdInvalid) as e:
        await _safe_edit(status_msg, f"{pfx}❌ Sin acceso al canal o mensaje `{msg_id}`: {e}")
        return

    if src is None:
        await _safe_edit(status_msg, f"{pfx}⚠️ Mensaje `{msg_id}` no existe.")
        return

    if not src.media:
        await bot.send_message(
            original_msg.chat.id,
            text=src.text or src.caption or "_(Mensaje sin contenido)_",
        )
        await _safe_edit(status_msg, f"{pfx}✅ Texto copiado.")
        return

    await _safe_edit(status_msg, f"{pfx}📋 Intentando copia directa...")
    try:
        await user.copy_message(
            chat_id=original_msg.chat.id,
            from_chat_id=chat_id,
            message_id=msg_id,
        )
        await _safe_edit(status_msg, f"{pfx}✅ Copiado correctamente.")
        return
    except (ChatForwardsRestricted, Exception):
        logger.info("copy_message bloqueado. Pasando a Streaming Directo.")

    await _safe_edit(status_msg, f"{pfx}⚡ Canal restringido. Iniciando transferencia directa...")
    await _stream_and_send(bot, user, original_msg, status_msg, src, pfx)


# ══════════════════════════════════════════════════════════════════════════════
# Streaming Directo (Piping)
# ══════════════════════════════════════════════════════════════════════════════

async def _stream_and_send(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    src: Message,
    pfx: str,
) -> None:
    total_size = _get_media_size(src)
    file_name = _get_media_name(src) or f"file_{src.id}"
    caption = src.caption or ""

    tracker = ProgressTracker(
        total_bytes=total_size,
        status_msg=status_msg,
        prefix=pfx,
        update_interval=PROGRESS_UPDATE_INTERVAL,
    )

    try:
        async def chunk_generator():
            async for chunk in user.stream_media(src):
                if chunk:
                    await tracker.update(len(chunk))
                    yield chunk

        # El objeto generador necesita un atributo 'name' para que Pyrogram sepa el nombre del archivo
        stream = chunk_generator()
        setattr(stream, "name", file_name)

        await _safe_edit(status_msg, f"{pfx}📤 Transfiriendo `{file_name}`...")
        await _dispatch_media(bot, original_msg.chat.id, src, stream, caption)
        await _safe_edit(status_msg, f"{pfx}✅ `{file_name}` entregado.")

    except Exception as e:
        logger.error("Error en streaming msg %d: %s", src.id, e, exc_info=True)
        await _safe_edit(status_msg, f"{pfx}❌ Error de transferencia: `{e}`")


async def _dispatch_media(
    bot: Client,
    chat_id: int,
    src: Message,
    stream: AsyncGenerator,
    caption: str,
) -> None:
    kw = dict(chat_id=chat_id, caption=caption)
    media_type = src.media.value if src.media else "document"

    if media_type == "video":
        await bot.send_video(video=stream, **kw)
    elif media_type == "audio":
        await bot.send_audio(audio=stream, **kw)
    elif media_type == "voice":
        await bot.send_voice(voice=stream, **kw)
    elif media_type == "photo":
        await bot.send_photo(photo=stream, **kw)
    elif media_type == "animation":
        await bot.send_animation(animation=stream, **kw)
    else:
        await bot.send_document(document=stream, **kw)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers y Reintentos
# ══════════════════════════════════════════════════════════════════════════════

async def _get_message_with_retry(user: Client, chat_id: str, msg_id: int, max_retries: int = 3) -> Optional[Message]:
    raw_id = str(chat_id).replace("c/", "")
    target_id = int(f"-100{raw_id}") if raw_id.isdigit() else chat_id

    try:
        await user.get_chat(target_id)
    except Exception:
        try:
            async for dialog in user.get_dialogs(limit=100):
                if str(dialog.chat.id) == str(target_id):
                    break
        except Exception:
            pass

    for attempt in range(max_retries):
        try:
            return await user.get_messages(target_id, msg_id)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except PeerIdInvalid:
            if attempt < max_retries - 1:
                try: await user.resolve_peer(target_id)
                except: pass
                await asyncio.sleep(2)
                continue
            raise
        except Exception:
            if attempt == max_retries - 1: raise
            await asyncio.sleep(2)
    return None

async def _safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit_text(text)
    except MessageNotModified:
        pass
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        await _safe_edit(msg, text)
    except Exception:
        pass

def _get_media_size(msg: Message) -> int:
    for attr in ("video", "audio", "document", "voice", "video_note", "animation", "sticker"):
        obj = getattr(msg, attr, None)
        if obj and hasattr(obj, "file_size") and obj.file_size:
            return obj.file_size
    return 0

def _get_media_name(msg: Message) -> Optional[str]:
    for attr in ("video", "audio", "document", "animation"):
        obj = getattr(msg, attr, None)
        if obj and getattr(obj, "file_name", None):
            return obj.file_name
    return None
