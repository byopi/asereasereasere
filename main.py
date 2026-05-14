import asyncio
import logging
import signal
import sys

import aiohttp
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


async def register_webhook() -> None:
    """
    Registra el webhook en Telegram usando la Bot API HTTP directamente.
    Pyrogram no tiene set_webhook — se llama al endpoint REST oficial.
    """
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/setWebhook"
    payload = {"url": config.WEBHOOK_URL}

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            data = await resp.json()
            if data.get("ok"):
                logger.info("✅ Webhook registrado en: %s", config.WEBHOOK_URL)
            else:
                logger.error("❌ Error registrando webhook: %s", data)
                raise RuntimeError(f"setWebhook falló: {data}")


async def delete_webhook() -> None:
    """Elimina el webhook al apagar el servidor."""
    url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/deleteWebhook"
    async with aiohttp.ClientSession() as session:
        async with session.post(url) as resp:
            data = await resp.json()
            logger.info("Webhook eliminado: %s", data.get("description", ""))


async def build_app(bot: Client, user: Client) -> web.Application:
    app = web.Application()
    app["bot"] = bot
    app["user"] = user
    from routes import setup_routes
    setup_routes(app)
    return app


async def main() -> None:
    user_client, bot_client = build_clients()
    register_handlers(bot_client, user_client)

    logger.info("Iniciando clientes de Pyrogram...")
    await user_client.start()
    await bot_client.start()
    logger.info("✅ Clientes conectados a Telegram")

    await register_webhook()

    app = await build_app(bot_client, user_client)
    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=config.PORT)
    await site.start()
    logger.info("🌐 Servidor HTTP en 0.0.0.0:%d", config.PORT)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Señal de apagado recibida...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info("Bot activo. Esperando updates vía webhook...")
    await stop_event.wait()

    logger.info("Apagando...")
    await delete_webhook()
    await runner.cleanup()
    await bot_client.stop()
    await user_client.stop()
    logger.info("Bot detenido.")


if __name__ == "__main__":
    asyncio.run(main())
