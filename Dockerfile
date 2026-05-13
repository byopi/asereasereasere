# ══════════════════════════════════════════════════════════════════════════════
# Dockerfile — Web Service mode para Render.com
#
# Diferencia clave vs Worker mode:
#   • El proceso escucha en el puerto $PORT (asignado por Render)
#   • Render hace health checks HTTP a /health
#   • Si /health no responde 2xx en 5 minutos → restart automático
#
# Estrategia de capas (cache-friendly):
#   Layer 1: SO + ffmpeg   (cambia nunca)
#   Layer 2: pip deps      (cambia en releases)
#   Layer 3: código fuente (cambia en cada commit)
# ══════════════════════════════════════════════════════════════════════════════

FROM python:3.10-slim

# ── Metadatos ──────────────────────────────────────────────────────────────────
LABEL maintainer="tg-dl-bot" \
      description="Telegram restricted content downloader — Web Service"

# ── Variables de entorno Python ────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ── Dependencias del sistema ───────────────────────────────────────────────────
# ffmpeg: requerido por Pyrogram para thumbnails de video y procesamiento de audio
# --no-install-recommends: reduce la imagen ~120 MB
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Directorio de trabajo ──────────────────────────────────────────────────────
WORKDIR /app

# ── Dependencias Python (capa cacheada) ───────────────────────────────────────
# Se copia SOLO requirements.txt para que Docker reutilice esta capa
# si el código fuente cambia pero las dependencias no.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Código fuente ──────────────────────────────────────────────────────────────
COPY . .

# ── Usuario no-root (seguridad) ────────────────────────────────────────────────
RUN useradd --no-create-home --shell /bin/false appuser \
    && chown -R appuser:appuser /app
USER appuser

# ── Puerto expuesto ────────────────────────────────────────────────────────────
# Render inyecta $PORT automáticamente. Lo exponemos para documentación.
# main.py lee config.PORT = int(os.getenv("PORT", "10000"))
EXPOSE 10000

# ── Comando de inicio ──────────────────────────────────────────────────────────
# exec form (sin shell): las señales SIGTERM llegan directamente al proceso Python
# -u: unbuffered stdout para que los logs aparezcan en tiempo real en Render
CMD ["python", "-u", "main.py"]
