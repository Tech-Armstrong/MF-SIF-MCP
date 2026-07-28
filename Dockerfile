# NAV Analytics MCP server — container image for Azure App Service (or any PaaS).
#
# App Service injects the listen port via $PORT; server.py reads it and binds
# 0.0.0.0. The MCP endpoint is served at /mcp. All configuration (Azure storage
# connection string, parquet paths, OAuth credentials) is passed as environment
# variables / application settings at deploy time — nothing secret is baked in.
FROM python:3.12-slim

WORKDIR /app

# Install deps first so the layer caches across code changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .

# Default port for local `docker run`; App Service overrides $PORT at runtime.
ENV PORT=8000
EXPOSE 8000

CMD ["python", "server.py"]
