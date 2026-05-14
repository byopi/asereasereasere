"""
handlers/download.py — Transferencia 0-RAM con os.pipe()

Arquitectura:
  stream_media() → pipe write → pipe read → send_video/send_document
  
Sin escribir en disco, sin cargar todo en RAM. Los pipes son buffers de kernel.
"""

import asyncio
import logging
import os
import io
from typing import Optional

from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from config import MAX_BATCH_SIZE, PROGRESS_UPDATE_INTERVAL
from utils.parser import parse_telegram_url
from utils.progress import ProgressTracker

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# /dl — Descarga un solo mensaje
# ══════════════════════════════════════════════════════════════════════════════

async def handle_dl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply_text("❌ **Uso:** `/dl <url_del_mensaje>`")
        return

    parsed = parse_telegram_url(args[1].strip())
    if not parsed:
        await message.reply_text("❌ URL inválida.")
        return

    chat_id, msg_id = parsed
    status = await message.reply_text("🔍 Localizando mensaje...")
    await _process_single(bot, user, message, status, chat_id, msg_id)


# ══════════════════════════════════════════════════════════════════════════════
# /bdl — Descarga por lotes
# ══════════════════════════════════════════════════════════════════════════════

async def handle_bdl(bot: Client, user: Client, message: Message) -> None:
    args = message.text.split()
    if len(args) < 3:
        await message.reply_text("❌ **Uso:** `/bdl <url_inicio> <url_fin>`")
        return

    p1 = parse_telegram_url(args[1])
    p2 = parse_telegram_url(args[2])

    if not p1 or not p2:
        await message.reply_text("❌ URLs inválidas.")
        return

    if p1[0] != p2[0]:
        await message.reply_text("❌ Las URLs deben ser del mismo canal.")
        return

    start_id, end_id = (p1[1], p2[1]) if p1[1] <= p2[1] else (p2[1], p1[1])
    total = end_id - start_id + 1

    if total > MAX_BATCH_SIZE:
        await message.reply_text(f"❌ Máximo {MAX_BATCH_SIZE} mensajes por lote.")
        return

    status = await message.reply_text(f"📦 Lote: `{start_id}` → `{end_id}` ({total} mensajes)")

    ok, fail = 0, 0
    for i, msg_id in enumerate(range(start_id, end_id + 1), 1):
        try:
            await _process_single(
                bot, user, message, status, p1[0], msg_id,
                pfx=f"[{i}/{total}]"
            )
            ok += 1
        except Exception as e:
            logger.warning("Error en msg %d: %s", msg_id, e)
            fail += 1

        await asyncio.sleep(1.5)

    await _safe_edit(status, f"✅ Lote completado: {ok} éxitos, {fail} fallos de {total}")


# ══════════════════════════════════════════════════════════════════════════════
# Núcleo: procesar un mensaje
# ══════════════════════════════════════════════════════════════════════════════

async def _process_single(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    chat_id: str,
    msg_id: int,
    pfx: str = "",
) -> None:
    """Obtiene el mensaje y lo envía (copy o streaming)."""
    pfx = f"{pfx} " if pfx else ""

    # Obtener el mensaje
    src = await _get_message_with_retry(user, chat_id, msg_id)
    if not src:
        await _safe_edit(status_msg, f"{pfx}⚠️ Mensaje `{msg_id}` no accesible.")
        return

    # Sin media: solo texto
    if not src.media:
        text = src.text or src.caption or "_(Sin contenido)_"
        await bot.send_message(original_msg.chat.id, text)
        await _safe_edit(status_msg, f"{pfx}✅ Texto copiado.")
        return

    # Con media: intentar copy primero
    try:
        await user.copy_message(original_msg.chat.id, chat_id, msg_id)
        await _safe_edit(status_msg, f"{pfx}✅ Mensaje copiado directamente.")
        return
    except Exception as e:
        logger.info("Copy bloqueado (%s). Usando pipes...", type(e).__name__)

    # Si copy falla: streaming con pipes
    await _safe_edit(status_msg, f"{pfx}⚡ Transfiriendo vía pipe...")
    await _stream_and_send(bot, user, original_msg, status_msg, src, pfx)


# ══════════════════════════════════════════════════════════════════════════════
# Streaming con pipes (0-RAM)
# ══════════════════════════════════════════════════════════════════════════════

class FileProxy(io.RawIOBase):
    """
    Proxy para Pyrogram que simula un archivo seekable en un pipe no-seekable.
    
    Pyrogram valida:
      • .file_size: tamaño total (evita "file size equals to 0 B")
      • .size: alias de file_size
      • __len__(): para el validador
      • seekable() / seek(): para videos grandes
      • read(n): para leer en chunks
    """

    def __init__(self, file_obj, custom_name: str, file_size: int):
        super().__init__()
        self._file = file_obj
        self.name = custom_name
        self.file_size = file_size
        self.size = file_size
        self._size = file_size
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        """Lee del pipe (no bloqueante)."""
        data = self._file.read(n)
        self._pos += len(data)
        return data

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def seek(self, offset: int, whence: int = 0) -> int:
        """Finge seek (necesario para videos)."""
        return 0

    def tell(self) -> int:
        """Posición actual."""
        return self._pos

    def __len__(self) -> int:
        """Pyrogram lo usa para validar el tamaño."""
        return self._size

    def close(self):
        """Cierra el archivo."""
        try:
            self._file.close()
        except:
            pass
        super().close()


async def _stream_and_send(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    src: Message,
    pfx: str,
) -> None:
    """
    Descarga en un thread y sube en paralelo (ambas tareas concurrentes).
    El pipe es el buffer — no toca disco ni RAM (mucho).
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

    # Crear pipe (lectura/escritura bidireccional)
    r, w = os.pipe()
    reader_fd = os.fdopen(r, "rb", buffering=0)
    writer_fd = os.fdopen(w, "wb", buffering=0)

    # Proxy para engañar a Pyrogram (que valide el tamaño antes de leer)
    proxy = FileProxy(reader_fd, file_name, total_size)

    # Tarea 1: descargar desde Telegram al pipe
    async def download_task():
        try:
            async for chunk in user.stream_media(src):
                if chunk:
                    writer_fd.write(chunk)
                    writer_fd.flush()
                    await tracker.update(len(chunk))
        except Exception as e:
            logger.error("Error descargando: %s", e)
            raise
        finally:
            try:
                writer_fd.close()
            except:
                pass

    # Tarea 2: subir desde el pipe a Telegram
    async def upload_task():
        try:
            # Esperar a que el pipe tenga datos (sleep mínimo)
            await asyncio.sleep(0.5)

            if total_size <= 0:
                raise ValueError("Tamaño de archivo no detectado (0 bytes)")

            await _safe_edit(status_msg, f"{pfx}📤 Subiendo...")
            await _dispatch_media(bot, original_msg.chat.id, src, proxy, caption)

        except Exception as e:
            logger.error("Error subiendo: %s", e)
            raise
        finally:
            try:
                proxy.close()
            except:
                pass

    # Ejecutar ambas tareas en paralelo
    try:
        await asyncio.gather(download_task(), upload_task())
        await _safe_edit(status_msg, f"{pfx}✅ `{file_name}` entregado.")

    except Exception as e:
        logger.error("Error en streaming: %s", e, exc_info=True)
        await _safe_edit(status_msg, f"{pfx}❌ Error: `{str(e)[:50]}`")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Despacho de media según tipo
# ══════════════════════════════════════════════════════════════════════════════

async def _dispatch_media(
    bot: Client,
    chat_id: int,
    src: Message,
    fp,
    caption: str,
) -> None:
    """Envía el archivo según su tipo de media."""
    kw = dict(chat_id=chat_id, caption=caption)
    media_type = src.media.value if src.media else "document"

    if media_type == "video":
        await bot.send_video(video=fp, **kw)
    elif media_type == "audio":
        await bot.send_audio(audio=fp, **kw)
    elif media_type == "voice":
        await bot.send_voice(voice=fp, **kw)
    elif media_type == "photo":
        await bot.send_photo(photo=fp, **kw)
    elif media_type == "animation":
        await bot.send_animation(animation=fp, **kw)
    else:
        await bot.send_document(document=fp, **kw)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _get_message_with_retry(
    user: Client,
    chat_id: str,
    msg_id: int,
    max_retries: int = 3,
) -> Optional[Message]:
    """Obtiene un mensaje con reintentos."""
    for attempt in range(max_retries):
        try:
            msg = await user.get_messages(chat_id, msg_id)
            if msg and not msg.empty:
                return msg
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception as e:
            logger.debug("Intento %d falló: %s", attempt + 1, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)

    return None


async def _safe_edit(msg: Message, text: str) -> None:
    """Edita un mensaje ignorando errores."""
    try:
        await msg.edit_text(text)
    except Exception:
        pass


def _get_media_size(msg: Message) -> int:
    """Extrae el tamaño total del media en bytes."""
    for attr in ("video", "audio", "document", "voice", "animation"):
        obj = getattr(msg, attr, None)
        if obj and hasattr(obj, "file_size") and obj.file_size:
            return obj.file_size
    return 0


def _get_media_name(msg: Message) -> Optional[str]:
    """Extrae el nombre del archivo."""
    for attr in ("video", "audio", "document", "animation"):
        obj = getattr(msg, attr, None)
        if obj and getattr(obj, "file_name", None):
            return obj.file_name
    return None
