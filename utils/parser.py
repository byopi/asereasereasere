"""
utils/parser.py — Parser de URLs de mensajes de Telegram.

Formatos soportados:
  https://t.me/canal/123          → canal público
  https://t.me/c/1234567890/123   → canal privado (ID numérico)
  t.me/canal/123                  → sin esquema https
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

    Los canales privados usan el formato -100XXXXXXXXXX que requiere la API MTProto.
    """
    url = url.strip()

    # Canal privado tiene precedencia (su regex es más específico)
    m = _PRIVATE_RE.search(url)
    if m:
        chat_id = f"-100{m.group(1)}"  # Formato requerido por la API
        return chat_id, int(m.group(2))

    m = _PUBLIC_RE.search(url)
    if m:
        return f"@{m.group(1)}", int(m.group(2))

    return None
