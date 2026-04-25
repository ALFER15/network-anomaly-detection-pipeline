from __future__ import annotations

"""Entry point local para levantar la API con Uvicorn."""

import uvicorn

from config import settings


if __name__ == "__main__":
    """Arranca la aplicación FastAPI usando la configuración del entorno."""
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=True,
    )
