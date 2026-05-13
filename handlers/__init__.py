"""
handlers/__init__.py — Registra todos los handlers de comandos en el bot_client.
En modo webhook, Pyrogram procesa el update JSON y dispara estos handlers
exactamente igual que en modo polling. La diferencia es de transporte, no de API.
"""

from pyrogram import Client, filters
from pyrogram.types import Message

from .download import handle_dl, handle_bdl


def register_handlers(bot: Client, user: Client) -> None:
    """Inyecta user_client en los handlers y los registra en el bot_client."""

    @bot.on_message(filters.command("start"))
    async def start_cmd(_, message: Message):
        await message.reply_text(
            "👋 **Bot de descarga de contenido restringido**\n\n"
            "**Comandos:**\n"
            "`/dl <url>` — Descarga un mensaje\n"
            "`/bdl <url_inicio> <url_fin>` — Descarga por lotes (máx. 50)\n\n"
            "📌 _Soporta canales públicos y privados (t.me/c/...)_\n"
            "⚡ _Streaming directo servidor↔servidor, sin tocar el disco_"
        )

    @bot.on_message(filters.command("dl") & filters.private)
    async def dl_cmd(_, message: Message):
        await handle_dl(bot, user, message)

    @bot.on_message(filters.command("bdl") & filters.private)
    async def bdl_cmd(_, message: Message):
        await handle_bdl(bot, user, message)
