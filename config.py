"""
config.py — Configuración centralizada vía variables de entorno.

Web Service Mode (Webhook):
  - Render.com expone un puerto HTTP público
  - Telegram envía updates a https://<tu-app>.onrender.com/webhook/<BOT_TOKEN>
  - Mucho más eficiente que long-polling: cero conexiones idle
"""

import os
from typing import Optional


def _require(key: str) -> str:
    """Lee una variable de entorno requerida; falla rápido si no existe."""
    value: Optional[str] = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Variable de entorno requerida no encontrada: '{key}'.\n"
            f"Configúrala en: Render Dashboard → tu servicio → Environment"
        )
    return value


# ── Credenciales de la API de Telegram ────────────────────────────────────────
API_ID: int = int(_require("API_ID"))
API_HASH: str = _require("API_HASH")
BOT_TOKEN: str = _require("BOT_TOKEN")
SESSION_STRING: str = _require("SESSION_STRING")

# ── Configuración del Web Service / Webhook ────────────────────────────────────
# Render asigna el puerto via variable PORT (por defecto 10000)
PORT: int = int(os.getenv("PORT", "10000"))

# URL pública del servicio en Render (ej: https://mi-bot.onrender.com)
# Render la inyecta automáticamente como RENDER_EXTERNAL_URL
WEBHOOK_HOST: str = _require("RENDER_EXTERNAL_URL")

# La URL completa del webhook que Telegram llamará con cada update
# El BOT_TOKEN en la ruta actúa como secreto: solo Telegram sabe la URL exacta
WEBHOOK_URL: str = f"{WEBHOOK_HOST.rstrip('/')}/webhook/{BOT_TOKEN}"

# ── Ajustes de rendimiento ─────────────────────────────────────────────────────
# 512 KB por chunk: equilibrio óptimo entre RAM (512 MB en plan free) y velocidad
CHUNK_SIZE: int = int(os.getenv("CHUNK_SIZE", str(512 * 1024)))

# Máximo de mensajes por lote en /bdl
MAX_BATCH_SIZE: int = int(os.getenv("MAX_BATCH_SIZE", "50"))

# Segundos entre actualizaciones de la barra de progreso (evita FloodWait)
PROGRESS_UPDATE_INTERVAL: float = float(os.getenv("PROGRESS_UPDATE_INTERVAL", "3.0"))
