FROM python:3.11.14-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	UV_LINK_MODE=copy \
	UV_PROJECT_ENVIRONMENT=/opt/venv \
	PATH="/opt/venv/bin:${PATH}"

RUN python -m pip install --no-cache-dir --upgrade pip \
	&& python -m pip install --no-cache-dir "uv>=0.8.0,<0.9.0"

WORKDIR /workspace/backend

# Dependency layer for faster local rebuilds.
COPY backend/pyproject.toml backend/uv.lock backend/CODEBASE_SUMMARY.md ./
RUN uv sync --locked --no-install-project --group cpu_dev

FROM base AS cpu-dev

WORKDIR /workspace/backend
COPY backend /workspace/backend
COPY shared /workspace/shared

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
