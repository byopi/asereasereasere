"""
routes.py — Rutas HTTP del Web Service.

Endpoints:
  POST /webhook/<token>  → Recibe updates de Telegram (el núcleo del webhook)
  GET  /health           → Health check para Render (responde 200 OK)
  GET  /                 → Dashboard de estado del bot
"""

import json
import logging
from datetime import datetime, timezone

from aiohttp import web
from pyrogram import Client
from pyrogram.types import Update

import config

logger = logging.getLogger(__name__)

# Timestamp de inicio del servidor (para el dashboard)
_START_TIME = datetime.now(timezone.utc)

# Contador de updates procesados
_stats = {"updates_received": 0, "errors": 0}


def setup_routes(app: web.Application) -> None:
    """Registra todas las rutas en la aplicación aiohttp."""
    app.router.add_post(f"/webhook/{config.BOT_TOKEN}", webhook_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", dashboard_handler)


# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK — El núcleo del Web Service
# ══════════════════════════════════════════════════════════════════════════════

async def webhook_handler(request: web.Request) -> web.Response:
    """
    Recibe el update JSON de Telegram, lo deserializa y lo pasa a Pyrogram.

    Telegram espera una respuesta 200 OK en < 5 segundos.
    Si el procesamiento tarda más (ej: streaming de archivos), se responde
    200 OK inmediatamente y el trabajo pesado se delega a una tarea asyncio.

    Flujo:
      Telegram → POST /webhook/<token>
               → Parsear JSON → Update de Pyrogram
               → Responder 200 OK (Telegram queda satisfecho)
               → asyncio.create_task() para procesar en background
    """
    bot: Client = request.app["bot"]

    try:
        data = await request.json()
        _stats["updates_received"] += 1
        logger.debug("Update recibido: %s", json.dumps(data)[:200])

        # Pasar el update a Pyrogram para que dispare los handlers registrados.
        # handle_update() es no-bloqueante: Pyrogram lo encola internamente.
        asyncio.create_task_safe(bot.handle_update(data))

        return web.Response(status=200, text="OK")

    except Exception as e:
        _stats["errors"] += 1
        logger.error("Error procesando update: %s", e)
        # Siempre responder 200 a Telegram (si respondemos 5xx, deja de enviar)
        return web.Response(status=200, text="OK")


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK — Requerido por Render para Web Services
# ══════════════════════════════════════════════════════════════════════════════

async def health_handler(request: web.Request) -> web.Response:
    """
    Render hace GET /health periódicamente.
    Si responde algo distinto de 2xx, Render marca el servicio como caído
    y lo reinicia. Esta ruta garantiza que el proceso está vivo y respondiendo.
    """
    uptime = (datetime.now(timezone.utc) - _START_TIME).total_seconds()
    return web.json_response({
        "status": "ok",
        "uptime_seconds": int(uptime),
        "updates_received": _stats["updates_received"],
        "errors": _stats["errors"],
    })


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD — Página de estado visible en el navegador
# ══════════════════════════════════════════════════════════════════════════════

async def dashboard_handler(request: web.Request) -> web.Response:
    """Muestra un dashboard HTML minimalista con el estado del bot."""
    uptime = (datetime.now(timezone.utc) - _START_TIME).total_seconds()
    hours, rem = divmod(int(uptime), 3600)
    mins, secs = divmod(rem, 60)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TG DL Bot — Estado</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: 'Courier New', monospace;
      background: #0d1117;
      color: #c9d1d9;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 2rem;
    }}
    .card {{
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 8px;
      padding: 2rem;
      max-width: 480px;
      width: 100%;
    }}
    .status-dot {{
      display: inline-block;
      width: 10px;
      height: 10px;
      background: #3fb950;
      border-radius: 50%;
      margin-right: 8px;
      animation: pulse 2s infinite;
    }}
    @keyframes pulse {{
      0%, 100% {{ opacity: 1; }}
      50% {{ opacity: 0.4; }}
    }}
    h1 {{ font-size: 1.2rem; color: #f0f6fc; margin-bottom: 1.5rem; }}
    .stat {{
      display: flex;
      justify-content: space-between;
      padding: 0.5rem 0;
      border-bottom: 1px solid #21262d;
      font-size: 0.9rem;
    }}
    .stat:last-child {{ border-bottom: none; }}
    .label {{ color: #8b949e; }}
    .value {{ color: #58a6ff; font-weight: bold; }}
    .commands {{
      margin-top: 1.5rem;
      padding: 1rem;
      background: #0d1117;
      border-radius: 6px;
      font-size: 0.85rem;
      color: #8b949e;
      line-height: 1.8;
    }}
    code {{ color: #79c0ff; }}
  </style>
</head>
<body>
  <div class="card">
    <h1><span class="status-dot"></span>TG Downloader Bot</h1>
    <div class="stat">
      <span class="label">Estado</span>
      <span class="value">✅ Activo</span>
    </div>
    <div class="stat">
      <span class="label">Uptime</span>
      <span class="value">{hours:02d}h {mins:02d}m {secs:02d}s</span>
    </div>
    <div class="stat">
      <span class="label">Updates recibidos</span>
      <span class="value">{_stats['updates_received']}</span>
    </div>
    <div class="stat">
      <span class="label">Errores</span>
      <span class="value">{_stats['errors']}</span>
    </div>
    <div class="stat">
      <span class="label">Webhook</span>
      <span class="value">Activo</span>
    </div>
    <div class="commands">
      <code>/dl &lt;url&gt;</code> — Descarga un mensaje<br>
      <code>/bdl &lt;url1&gt; &lt;url2&gt;</code> — Descarga por lotes
    </div>
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ── Helper: asyncio.create_task con logging de errores ────────────────────────
import asyncio

def asyncio_create_task_safe(coro):
    """Crea una task asyncio que loggea excepciones en lugar de silenciarlas."""
    task = asyncio.create_task(coro)
    task.add_done_callback(_log_task_exception)
    return task

def _log_task_exception(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        logger.error("Excepción en background task: %s", task.exception())

# Monkey-patch para el uso en webhook_handler
import builtins
asyncio.create_task_safe = asyncio_create_task_safe
