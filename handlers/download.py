"""
handlers/download.py — Lógica de descarga con streaming Zero-Disk.

Gestión de memoria (por qué no hay OOM con archivos grandes):
─────────────────────────────────────────────────────────────
  `client.stream_media()` es un generador asíncrono que hace requests
  HTTP al CDN de Telegram y yield-ea chunks sin cargarlos todos en RAM.

  Cada chunk (512 KB por defecto) se escribe en un `io.BytesIO`.
  El BytesIO crece hasta el tamaño total del archivo, luego se hace
  seek(0) y se pasa a send_document/send_video/etc.

  ┌─────────────────────────────────────────────────────────────┐
  │  RAM usada ≈ tamaño del archivo + overhead de Pyrogram      │
  │  Disco usado = 0 bytes (nunca se llama a open())            │
  └─────────────────────────────────────────────────────────────┘

  Para el plan FREE de Render (512 MB RAM):
    • Archivos < 400 MB → funcionan con margen
    • Archivos > 400 MB → usa plan Starter (2 GB RAM)
    • Archivos > 2 GB   → Telegram impone este límite en su API de bots

Fallback logic:
  1. copy_message()  → copia sin tráfico extra (ideal si funciona)
  2. stream_media()  → si copy falla por restricciones del canal
"""

import asyncio
import io
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

        # Pausa entre mensajes para no saturar la API
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
    """
    Estrategia de dos pasos:
      1. copy_message → rápido, sin overhead
      2. stream_media → si el canal bloquea el reenvío
    """
    pfx = f"{batch_prefix} " if batch_prefix else ""

    # ── Obtener el mensaje con el cliente de usuario ───────────────────────
    try:
        src = await _get_message_with_retry(user, chat_id, msg_id)
    except (MessageIdInvalid, ChannelPrivate, PeerIdInvalid) as e:
        await _safe_edit(status_msg, f"{pfx}❌ Sin acceso al canal o mensaje `{msg_id}`: {e}")
        return

    if src is None:
        await _safe_edit(status_msg, f"{pfx}⚠️ Mensaje `{msg_id}` no existe o fue eliminado.")
        return

    # ── Solo texto (sin media) → copiar directamente ──────────────────────
    if not src.media:
        await bot.send_message(
            original_msg.chat.id,
            text=src.text or src.caption or "_(Mensaje sin contenido)_",
        )
        await _safe_edit(status_msg, f"{pfx}✅ Texto del mensaje `{msg_id}` copiado.")
        return

    # ── Intento 1: copy_message ────────────────────────────────────────────
    await _safe_edit(status_msg, f"{pfx}📋 Intentando copia directa...")
    try:
        await user.copy_message(
            chat_id=original_msg.chat.id,
            from_chat_id=chat_id,
            message_id=msg_id,
        )
        await _safe_edit(status_msg, f"{pfx}✅ Mensaje `{msg_id}` copiado.")
        return
    except (ChatForwardsRestricted, Exception) as e:
        logger.info("copy_message bloqueado para %s/%d (%s). Pasando a streaming.", chat_id, msg_id, type(e).__name__)

    # ── Intento 2: Streaming Zero-Disk ────────────────────────────────────
    await _safe_edit(status_msg, f"{pfx}⚡ Canal restringido. Iniciando streaming en memoria...")
    await _stream_and_send(bot, user, original_msg, status_msg, src, pfx)


# ══════════════════════════════════════════════════════════════════════════════
# Streaming Zero-Disk: RAM buffer → Telegram upload
# ══════════════════════════════════════════════════════════════════════════════

async def _stream_and_send(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    src: Message,
    pfx: str,
) -> None:
    """
    Descarga el media chunk a chunk en un io.BytesIO (RAM).
    Cuando termina, lo sube a Telegram directamente desde RAM.

    io.BytesIO actúa como un archivo en memoria:
      • .write(chunk) → acumula bytes
      • .seek(0)      → rebobina para lectura
      • .close()      → GC libera la RAM (sin archivo que borrar)
    """
    total_size = _get_media_size(src)
    file_name = _get_media_name(src) or f"file_{src.id}"
    caption = src.caption or ""

    tracker = ProgressTracker(
        total_bytes=total_size,
        status_msg=status_msg,
        prefix=pfx,
        update_interval=PROGRESS_UPDATE_INTERVAL,
    )

    # ── Buffer en RAM ──────────────────────────────────────────────────────
    # Nunca se llama a open(). Este objeto vive exclusivamente en heap memory.
    buffer = io.BytesIO()
    buffer.name = file_name  # Pyrogram usa .name como nombre de archivo

    try:
        async for chunk in _iter_chunks(user, src):
            buffer.write(chunk)
            await tracker.update(len(chunk))

        # Rebobinar obligatorio antes de pasar a Pyrogram
        buffer.seek(0)

        await _safe_edit(status_msg, f"{pfx}📤 Subiendo `{file_name}`...")
        await _dispatch_media(bot, original_msg.chat.id, src, buffer, caption)
        await _safe_edit(status_msg, f"{pfx}✅ `{file_name}` entregado.")

    except Exception as e:
        logger.error("Error en streaming msg %d: %s", src.id, e, exc_info=True)
        await _safe_edit(status_msg, f"{pfx}❌ Error durante el streaming: `{e}`")
        raise
    finally:
        buffer.close()  # Liberar RAM explícitamente


async def _iter_chunks(user: Client, msg: Message) -> AsyncGenerator[bytes, None]:
    """
    Generador asíncrono sobre stream_media().
    Cada `yield` trae CHUNK_SIZE bytes del servidor de Telegram.
    En ningún momento se guarda más de un chunk a la vez en esta función.
    """
    async for chunk in user.stream_media(msg, chunk_size=CHUNK_SIZE):
        yield chunk


async def _dispatch_media(
    bot: Client,
    chat_id: int,
    src: Message,
    buffer: io.BytesIO,
    caption: str,
) -> None:
    """Envía el buffer según el tipo de media del mensaje original."""
    kw = dict(chat_id=chat_id, caption=caption)
    media_type = src.media.value if src.media else "document"

    dispatch = {
        "video":      lambda: bot.send_video(video=buffer, **kw),
        "audio":      lambda: bot.send_audio(audio=buffer, **kw),
        "voice":      lambda: bot.send_voice(voice=buffer, **kw),
        "photo":      lambda: bot.send_photo(photo=buffer, **kw),
        "animation":  lambda: bot.send_animation(animation=buffer, **kw),
        "video_note": lambda: bot.send_video_note(video_note=buffer, **kw),
    }

    send_fn = dispatch.get(media_type)
    if send_fn:
        await send_fn()
    else:
        # sticker, document, cualquier tipo desconocido
        await bot.send_document(document=buffer, **kw)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _get_message_with_retry(
    user: Client,
    chat_id: str,
    msg_id: int,
    max_retries: int = 3,
) -> Optional[Message]:
    """Obtiene un mensaje con reintentos y corrección de ID para canales."""
    
    # ── CORRECCIÓN DE ID ──
    # Si el chat_id es numérico y no empieza con -100, se lo ponemos.
    # Esto soluciona el PEER_ID_INVALID en enlaces tipo /c/
    str_chat_id = str(chat_id)
    if str_chat_id.isdigit() and not str_chat_id.startswith("-100"):
        chat_id = int(f"-100{str_chat_id}")
    elif str_chat_id.startswith("c/"): # Por si el parser trae la 'c/'
        clean_id = str_chat_id.replace("c/", "")
        chat_id = int(f"-100{clean_id}")

    # Forzar al cliente a reconocer el chat
    try:
        await user.get_chat(chat_id)
    except Exception as e:
        logger.debug("No se pudo pre-obtener chat %s: %s", chat_id, e)

    for attempt in range(max_retries):
        try:
            return await user.get_messages(chat_id, msg_id)
        except FloodWait as fw:
            wait = fw.value + 2
            await asyncio.sleep(wait)
        except (MessageIdInvalid, ChannelPrivate):
            raise
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            await asyncio.sleep(wait)
    return None


async def _safe_edit(msg: Message, text: str) -> None:
    """Edita un mensaje ignorando errores de contenido idéntico."""
    try:
        await msg.edit_text(text)
    except MessageNotModified:
        pass
    except FloodWait as fw:
        await asyncio.sleep(fw.value)
        await _safe_edit(msg, text)
    except Exception as e:
        logger.debug("No se pudo editar mensaje de estado: %s", e)


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
