import asyncio
import logging
import signal
import sys
import os
from aiohttp import web
from pyrogram import Client, idle

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
    user_client = Client(
        name="user",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        session_string=config.SESSION_STRING,
        no_updates=True,
    )

    bot_client = Client(
        name="bot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN,
    )

    return user_client, bot_client


# ══════════════════════════════════════════════════════════════════════════════
# SERVIDOR AIOHTTP (Web Service para Keep-Alive / UptimeRobot)
# ══════════════════════════════════════════════════════════════════════════════

async def handle(request):
    """Respuesta para UptimeRobot y Render"""
    return web.Response(text="Bot Universo Football Activo 🚀")

async def build_app(bot: Client, user: Client) -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app["user"] = user
    
    # Ruta principal para que UptimeRobot vea que el bot está vivo
    app.router.add_get("/", handle)
    
    # Tus rutas originales (si existen en routes.py)
    try:
        from routes import setup_routes
        setup_routes(app)
    except ImportError:
        pass

    return app


# ══════════════════════════════════════════════════════════════════════════════
# CICLO DE VIDA DE LA APLICACIÓN
# ══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    user_client, bot_client = build_clients()

    # Registrar todos los handlers de comandos
    register_handlers(bot_client, user_client)

    logger.info("Iniciando clientes de Pyrogram...")
    try:
        await user_client.start()
        await bot_client.start()
        logger.info("✅ Clientes conectados a Telegram")

        # Construir y arrancar el servidor HTTP (Sustituye al webhook para evitar errores)
        app = await build_app(bot_client, user_client)
        runner = web.AppRunner(app)
        await runner.setup()

        # Render usa la variable PORT, si no existe usa 8080
        port = int(os.environ.get("PORT", 8080))
        site = web.TCPSite(runner, host="0.0.0.0", port=port)
        await site.start()
        logger.info("🌐 Servidor HTTP Keep-Alive escuchando en el puerto: %d", port)

        # Manejar señales de apagado limpio
        stop_event = asyncio.Event()
        def _signal_handler():
            logger.info("Señal de apagado recibida...")
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        logger.info("Bot activo 24/7. Esperando mensajes...")
        
        # Combinamos idle() con nuestro stop_event
        await stop_event.wait()

    except Exception as e:
        logger.error(f"❌ Error durante la ejecución: {e}")
    
    finally:
        # ── Apagado limpio (Sin stop_webhook que daba error) ──────────────────
        logger.info("Apagando servidor y clientes...")
        try:
            await runner.cleanup()
            await bot_client.stop()
            await user_client.stop()
        except:
            pass
        logger.info("Bot detenido correctamente.")


if __name__ == "__main__":
    # Evita el error de 'Event loop is closed' en algunos entornos
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
