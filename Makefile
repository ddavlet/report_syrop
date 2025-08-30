# ---- Config ----
PY ?= python3
PIP ?= $(PY) -m pip
VENVDIR ?= .venv
PYTHON ?= $(VENVDIR)/bin/python
PIPV ?= $(VENVDIR)/bin/pip

ENV ?= .env



# Docker
DOCKER_COMPOSE ?= docker compose
APP_SERVICE ?= app
DB_SERVICE ?= db
DATA_LOADER_SERVICE ?= data_loader

.PHONY: help venv install up down clean clean-venv clean-volumes rebuild recreate bot data-loader

help:
	@echo "Commands:"
	@echo "  make venv                 - Create virtual environment"
	@echo "  make install              - Install requirements into venv"
	@echo "  make up                   - Start Postgres via docker compose"
	@echo "  make down                 - Stop all services"
	@echo "  make bot                  - Run Telegram bot locally"
	@echo "  make data-loader          - Run data loader service locally"
	@echo "  make clean                - Remove caches and temporary files"
	@echo "  make clean-venv           - Remove virtual environment"
	@echo "  make clean-volumes        - Stop and remove containers and volumes"
	@echo "  make rebuild              - Force rebuild images (no cache)"
	@echo "  make recreate             - Rebuild containers and start services"

venv:
	$(PY) -m venv $(VENVDIR)

install: venv
	$(PIPV) install -U pip
	$(PIPV) install -r requirements.txt

# --- Docker lifecycle ---
up:
	$(DOCKER_COMPOSE) up -d $(DB_SERVICE)

down:
	$(DOCKER_COMPOSE) down

# --- Services ---
bot: install
	$(PYTHON) -m src.bot

data-loader: install
	$(PYTHON) -m src.data_loader_service



# --- Cleanup ---
clean:
	find . -type d -name '__pycache__' -exec rm -rf {} + || true
	find . -type f -name '*.pyc' -delete || true
	find . -type f -name '*.pyo' -delete || true

clean-venv:
	rm -rf $(VENVDIR)

clean-volumes:
	$(DOCKER_COMPOSE) down -v --remove-orphans || true
	$(DOCKER_COMPOSE) rm -fsv || true

# --- Recreate / Rebuild ---
rebuild:
	$(DOCKER_COMPOSE) build --no-cache

recreate: rebuild up
