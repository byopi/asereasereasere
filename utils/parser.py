"""
utils/parser.py — Parser de URLs de Telegram.

Formatos soportados:
  https://t.me/canal/123          → canal público (@canal)
  https://t.me/c/1234567890/123   → canal privado
  t.me/canal/123                  → sin esquema

Para canales privados: t.me/c/3779052214/27 → ID: -1003779052214
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
      - URL: t.me/c/3779052214/27
      - ID que Pyrogram espera: -1003779052214
      - Lógica: -100 + número del canal
    """
    url = url.strip()

    # ── Canal privado (tiene precedencia) ──────────────────────────────────
    m = _PRIVATE_RE.search(url)
    if m:
        raw_id = m.group(1)
        msg_id = int(m.group(2))
        # Formato correcto: -100 + ID del canal
        chat_id = f"-100{raw_id}"
        return chat_id, msg_id

    # ── Canal público ─────────────────────────────────────────────────────
    m = _PUBLIC_RE.search(url)
    if m:
        username = m.group(1)
        msg_id = int(m.group(2))
        return f"@{username}", msg_id

    return None
