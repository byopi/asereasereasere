"""
main.py — Punto de entrada del Web Service.

Arquitectura webhook:
─────────────────────
  Telegram → POST /webhook/<token> → aiohttp server → Pyrogram Application
                                           │
                                    GET  /health  → {"status": "ok"}
                                    GET  /         → Dashboard HTML

Por qué webhook > long-polling en Render:
  • Long-polling mantiene una conexión TCP idle permanente con Telegram
    → Render puede matar el proceso por inactividad (plan free)
  • Webhook: Telegram llama cuando hay mensajes; el servidor responde
    → El proceso siempre tiene actividad HTTP real
  • Latencia menor: el update llega en ms, no en el próximo ciclo de poll
"""

import asyncio
import logging
import signal
import sys
from aiohttp import web
from pyrogram import Client

import config
from handlers import register_handlers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CLIENTES PYROGRAM
# ══════════════════════════════════════════════════════════════════════════════

def build_clients() -> tuple[Client, Client]:
    """
    Crea los dos clientes Pyrogram.

    user_client: Sesión de usuario real → puede leer canales restringidos.
                 no_updates=True porque NO recibe updates de Telegram;
                 solo hace get_messages() y stream_media() bajo demanda.

    bot_client:  Sesión de bot → recibe updates vía webhook y envía mensajes.
                 En modo webhook, los updates llegan por HTTP, no por polling.
    """
    user_client = Client(
        name="user",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.SESSION_STRING,
        no_updates=True,  # Este cliente solo descarga, no recibe updates
    )

    bot_client = Client(
        name="bot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN,
    )

    return user_client, bot_client


# ══════════════════════════════════════════════════════════════════════════════
# SERVIDOR AIOHTTP (Web Service)
# ══════════════════════════════════════════════════════════════════════════════

async def build_app(bot: Client, user: Client) -> web.Application:
    """Construye la aplicación aiohttp con todas sus rutas."""
    app = web.Application()

    # Inyectar clientes en el estado de la app para accederlos en los handlers
    app["bot"] = bot
    app["user"] = user

    # Importar aquí para evitar importación circular
    from routes import setup_routes
    setup_routes(app)

    return app


async def start_webhook(bot: Client) -> None:
    """Registra el webhook en Telegram."""
    logger.info("Registrando webhook en: %s", config.WEBHOOK_URL)
    await bot.set_webhook(config.WEBHOOK_URL)
    logger.info("✅ Webhook registrado correctamente")


async def stop_webhook(bot: Client) -> None:
    """Elimina el webhook de Telegram al apagar el servidor."""
    logger.info("Eliminando webhook de Telegram...")
    await bot.delete_webhook()


# ══════════════════════════════════════════════════════════════════════════════
# CICLO DE VIDA DE LA APLICACIÓN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    user_client, bot_client = build_clients()

    # Registrar todos los handlers de comandos en el bot_client
    register_handlers(bot_client, user_client)

    logger.info("Iniciando clientes de Pyrogram...")
    await user_client.start()
    await bot_client.start()
    logger.info("✅ Clientes conectados a Telegram")

    # Registrar webhook en Telegram
    await start_webhook(bot_client)

    # Construir y arrancar el servidor HTTP
    app = await build_app(bot_client, user_client)
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=config.PORT)
    await site.start()
    logger.info("🌐 Servidor HTTP escuchando en 0.0.0.0:%d", config.PORT)

    # Manejar señales de apagado limpio (SIGTERM de Render, SIGINT local)
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Señal de apagado recibida...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info("Bot activo. Esperando updates vía webhook...")
    await stop_event.wait()

    # ── Apagado limpio ─────────────────────────────────────────────────────
    logger.info("Apagando servidor...")
    await stop_webhook(bot_client)
    await runner.cleanup()
    await bot_client.stop()
    await user_client.stop()
    logger.info("Bot detenido correctamente.")


if __name__ == "__main__":
    asyncio.run(main())
