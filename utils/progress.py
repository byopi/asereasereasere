"""
utils/progress.py — Barra de progreso en texto con throttling anti-FloodWait.

Telegram limita las ediciones de mensajes a ~20 por minuto por chat.
ProgressTracker garantiza que nunca se supere este límite usando un
intervalo mínimo de `update_interval` segundos entre ediciones.
"""

import asyncio
import logging
import time
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import Message

logger = logging.getLogger(__name__)

_BAR_LEN = 12  # Longitud visual de la barra de progreso


class ProgressTracker:
    def __init__(
        self,
        total_bytes: int,
        status_msg: Message,
        prefix: str = "",
        update_interval: float = 3.0,
    ):
        self.total = total_bytes
        self.received = 0
        self.status_msg = status_msg
        self.prefix = prefix
        self.update_interval = update_interval
        self._last_edit: float = 0.0
        self._t0: float = time.monotonic()

    async def update(self, chunk_bytes: int) -> None:
        """Acumula bytes y actualiza el mensaje si ha pasado suficiente tiempo."""
        self.received += chunk_bytes
        if time.monotonic() - self._last_edit < self.update_interval:
            return
        self._last_edit = time.monotonic()
        await self._render()

    async def _render(self) -> None:
        text = self._build_text()
        try:
            await self.status_msg.edit_text(text)
        except MessageNotModified:
            pass
        except FloodWait as fw:
            logger.warning("FloodWait en progreso: %ds", fw.value)
            await asyncio.sleep(fw.value)
        except Exception as e:
            logger.debug("Progress update ignorado: %s", e)

    def _build_text(self) -> str:
        """
        Construye el texto de progreso:

        [🔵 prefix] ⬇️ Descargando en memoria...
        [████████░░░░] 67.3%
        📦 128.5 MB / 192.0 MB  ·  ⚡ 2.3 MB/s
        """
        elapsed = max(time.monotonic() - self._t0, 0.01)
        speed = self.received / elapsed

        if self.total > 0:
            pct = min(self.received / self.total, 1.0)
            filled = int(_BAR_LEN * pct)
            bar = "█" * filled + "░" * (_BAR_LEN - filled)
            pct_str = f"{pct * 100:.1f}%"
            total_str = _fmt(self.total)
        else:
            bar = "░" * _BAR_LEN
            pct_str = "?"
            total_str = "?"

        pfx = f"{self.prefix} " if self.prefix else ""
        return (
            f"{pfx}⬇️ **Descargando en memoria...**\n"
            f"`[{bar}]` {pct_str}\n"
            f"📦 `{_fmt(self.received)}` / `{total_str}`  ·  ⚡ `{_fmt(speed)}/s`"
        )


def _fmt(size: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"
