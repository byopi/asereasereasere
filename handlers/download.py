"""
handlers/download.py — Transferencia 0-RAM con os.pipe()
Con soporte para canales privados y validación de peer.
"""

import asyncio
import logging
import os
import io
from typing import Optional

from pyrogram import Client
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelPrivate, MessageIdInvalid
from pyrogram.types import Message

from config import MAX_BATCH_SIZE, PROGRESS_UPDATE_INTERVAL
from utils.parser import parse_telegram_url
from utils.progress import ProgressTracker

logger = logging.getLogger(__name__)


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


async def _process_single(
    bot: Client,
    user: Client,
    original_msg: Message,
    status_msg: Message,
    chat_id: str,
    msg_id: int,
    pfx: str = "",
) -> None:
    pfx = f"{pfx} " if pfx else ""

    src = await _get_message_with_retry(user, chat_id, msg_id)
    if not src:
        await _safe_edit(status_msg, f"{pfx}⚠️ No puedo acceder a mensaje `{msg_id}`.\n_¿La sesión es válida? ¿Estás en el canal?_")
        return

    if not src.media:
        text = src.text or src.caption or "_(Sin contenido)_"
        await bot.send_message(original_msg.chat.id, text)
        await _safe_edit(status_msg, f"{pfx}✅ Texto copiado.")
        return

    try:
        await user.copy_message(original_msg.chat.id, chat_id, msg_id)
        await _safe_edit(status_msg, f"{pfx}✅ Mensaje copiado directamente.")
        return
    except Exception as e:
        logger.info("Copy bloqueado (%s). Usando pipes...", type(e).__name__)

    await _safe_edit(status_msg, f"{pfx}⚡ Transfiriendo vía pipe...")
    await _stream_and_send(bot, user, original_msg, status_msg, src, pfx)


class FileProxy(io.RawIOBase):
    def __init__(self, file_obj, custom_name: str, file_size: int):
        super().__init__()
        self._file = file_obj
        self.name = custom_name
        self.file_size = file_size
        self.size = file_size
        self._size = file_size
        self._pos = 0

    def read(self, n: int = -1) -> bytes:
        data = self._file.read(n)
        self._pos += len(data)
        return data

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def seek(self, offset: int, whence: int = 0) -> int:
        return 0

    def tell(self) -> int:
        return self._pos

    def __len__(self) -> int:
        return self._size

    def close(self):
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
    total_size = _get_media_size(src)
    file_name = _get_media_name(src) or f"file_{src.id}"
    caption = src.caption or ""

    tracker = ProgressTracker(
        total_bytes=total_size,
        status_msg=status_msg,
        prefix=pfx,
        update_interval=PROGRESS_UPDATE_INTERVAL,
    )

    r, w = os.pipe()
    reader_fd = os.fdopen(r, "rb", buffering=0)
    writer_fd = os.fdopen(w, "wb", buffering=0)

    proxy = FileProxy(reader_fd, file_name, total_size)

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

    async def upload_task():
        try:
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

    try:
        await asyncio.gather(download_task(), upload_task())
        await _safe_edit(status_msg, f"{pfx}✅ `{file_name}` entregado.")

    except Exception as e:
        logger.error("Error en streaming: %s", e, exc_info=True)
        await _safe_edit(status_msg, f"{pfx}❌ Error: `{str(e)[:50]}`")
        raise


async def _dispatch_media(
    bot: Client,
    chat_id: int,
    src: Message,
    fp,
    caption: str,
) -> None:
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


async def _get_message_with_retry(
    user: Client,
    chat_id: str,
    msg_id: int,
    max_retries: int = 3,
) -> Optional[Message]:
    """
    Obtiene un mensaje. 
    
    Si el chat_id es un canal privado (-100...), primero intenta
    "conocer" el canal antes de acceder a los mensajes.
    """
    logger.info("Obteniendo mensaje %d del chat %s", msg_id, chat_id)

    # Si es un ID numérico de canal privado, intenta hacer "join" primero
    if isinstance(chat_id, str) and chat_id.startswith("-100"):
        try:
            logger.info("Pre-validando acceso al canal %s", chat_id)
            await user.get_chat(chat_id)
            logger.info("✅ Acceso validado al canal")
        except Exception as e:
            logger.warning("No se puede acceder al canal (%s): %s", type(e).__name__, e)
            return None

    for attempt in range(max_retries):
        try:
            msg = await user.get_messages(chat_id, msg_id)
            if msg and not msg.empty:
                logger.info("✅ Mensaje %d obtenido", msg_id)
                return msg
            else:
                logger.warning("Mensaje vacío o no existe: %d", msg_id)
                return None

        except (MessageIdInvalid, ChannelPrivate) as e:
            logger.error("Error de acceso (%s): %s", type(e).__name__, e)
            return None

        except PeerIdInvalid as e:
            logger.error("PEER_ID_INVALID — La sesión no tiene acceso a este canal. %s", e)
            return None

        except FloodWait as fw:
            logger.warning("FloodWait: esperando %d segundos", fw.value)
            await asyncio.sleep(fw.value + 1)

        except Exception as e:
            logger.warning("Intento %d/%d falló (%s): %s", 
                          attempt + 1, max_retries, type(e).__name__, e)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error("Agotados los reintentos para mensaje %d", msg_id)
                return None

    return None


async def _safe_edit(msg: Message, text: str) -> None:
    try:
        await msg.edit_text(text)
    except Exception:
        pass


def _get_media_size(msg: Message) -> int:
    for attr in ("video", "audio", "document", "voice", "animation"):
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
