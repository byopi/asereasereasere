"""
utils/parser.py — Parser de URLs de Telegram con soporte para canales privados.

Formatos soportados:
  https://t.me/canal/123          → canal público (@canal)
  https://t.me/c/1234567890/123   → canal privado
  t.me/canal/123                  → sin esquema

El truco: para canales privados, el ID debe ser negativo pero SIN el -100.
Pyrogram lo maneja internamente.
"""

import re
from typing import Optional, Tuple

# Canal público: t.me/username/msg_id
_PUBLIC_RE = re.compile(
    r"(?:https?://)?t\.me/([a-zA-Z][a-zA-Z0-9_]{3,})/(\d+)",
    re.IGNORECASE,
)

# Canal privado: t.me/c/channel_id/msg_id
_PRIVATE_RE = re.compile(
    r"(?:https?://)?t\.me/c/(\d+)/(\d+)",
    re.IGNORECASE,
)


def parse_telegram_url(url: str) -> Optional[Tuple[str, int]]:
    """
    Devuelve (chat_id, message_id) o None si la URL es inválida.

    Para canales privados:
      - La URL tiene: t.me/c/3779052214/27
      - Extraemos: 3779052214 (el número del canal)
      - Lo convertimos a: -3779052214 (negativo, sin el -100)
      - Pyrogram lo reconoce correctamente
    """
    url = url.strip()

    # ── Canal privado (tiene precedencia) ──────────────────────────────────
    m = _PRIVATE_RE.search(url)
    if m:
        raw_id = m.group(1)
        msg_id = int(m.group(2))
        # Convertir a formato que Pyrogram entiende: negativo sin -100
        chat_id = f"-{raw_id}"
        return chat_id, msg_id

    # ── Canal público ─────────────────────────────────────────────────────
    m = _PUBLIC_RE.search(url)
    if m:
        username = m.group(1)
        msg_id = int(m.group(2))
        return f"@{username}", msg_id

    return None
