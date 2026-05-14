import asyncio
import logging
from datetime import datetime, timezone

from aiohttp import web
from pyrogram import Client
from pyrogram.types import Update

import config

logger = logging.getLogger(__name__)

_START_TIME = datetime.now(timezone.utc)
_stats = {"updates_received": 0, "errors": 0}


def setup_routes(app: web.Application) -> None:
    app.router.add_post(f"/webhook/{config.BOT_TOKEN}", webhook_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/", dashboard_handler)


async def webhook_handler(request: web.Request) -> web.Response:
    """
    Recibe el JSON de Telegram y lo procesa con Pyrogram.
    Responde 200 OK inmediatamente; el procesamiento ocurre en background.
    """
    bot: Client = request.app["bot"]

    try:
        data = await request.json()
        _stats["updates_received"] += 1

        # Pyrogram procesa el dict del update directamente
        asyncio.ensure_future(
            bot.handle_updates(data)
        )

        return web.Response(status=200, text="OK")

    except Exception as e:
        _stats["errors"] += 1
        logger.error("Error procesando update: %s", e)
        # Siempre 200 a Telegram — si devuelves 5xx deja de enviar updates
        return web.Response(status=200, text="OK")


async def health_handler(request: web.Request) -> web.Response:
    uptime = (datetime.now(timezone.utc) - _START_TIME).total_seconds()
    return web.json_response({
        "status": "ok",
        "uptime_seconds": int(uptime),
        "updates_received": _stats["updates_received"],
        "errors": _stats["errors"],
    })


async def dashboard_handler(request: web.Request) -> web.Response:
    uptime = (datetime.now(timezone.utc) - _START_TIME).total_seconds()
    hours, rem = divmod(int(uptime), 3600)
    mins, secs = divmod(rem, 60)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TG DL Bot</title>
  <style>
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:'Courier New',monospace;background:#0d1117;color:#c9d1d9;
         min-height:100vh;display:flex;align-items:center;justify-content:center;padding:2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:2rem;max-width:480px;width:100%}}
    .dot{{display:inline-block;width:10px;height:10px;background:#3fb950;border-radius:50%;
          margin-right:8px;animation:pulse 2s infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
    h1{{font-size:1.2rem;color:#f0f6fc;margin-bottom:1.5rem}}
    .stat{{display:flex;justify-content:space-between;padding:.5rem 0;
           border-bottom:1px solid #21262d;font-size:.9rem}}
    .stat:last-child{{border-bottom:none}}
    .label{{color:#8b949e}}.value{{color:#58a6ff;font-weight:bold}}
    .cmds{{margin-top:1.5rem;padding:1rem;background:#0d1117;border-radius:6px;
           font-size:.85rem;color:#8b949e;line-height:1.8}}
    code{{color:#79c0ff}}
  </style>
</head>
<body>
  <div class="card">
    <h1><span class="dot"></span>TG Downloader Bot</h1>
    <div class="stat"><span class="label">Estado</span><span class="value">✅ Activo</span></div>
    <div class="stat"><span class="label">Uptime</span><span class="value">{hours:02d}h {mins:02d}m {secs:02d}s</span></div>
    <div class="stat"><span class="label">Updates recibidos</span><span class="value">{_stats['updates_received']}</span></div>
    <div class="stat"><span class="label">Errores</span><span class="value">{_stats['errors']}</span></div>
    <div class="stat"><span class="label">Webhook</span><span class="value">Activo</span></div>
    <div class="cmds">
      <code>/dl &lt;url&gt;</code> — Descarga un mensaje<br>
      <code>/bdl &lt;url1&gt; &lt;url2&gt;</code> — Descarga por lotes
    </div>
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")
