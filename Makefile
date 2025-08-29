# ---- Config ----
PY ?= python3
PIP ?= $(PY) -m pip
VENVDIR ?= .venv
PYTHON ?= $(VENVDIR)/bin/python
PIPV ?= $(VENVDIR)/bin/pip

ENV ?= .env

# Data load params
JSON ?= sales.json
TABLE ?= sales
BACKEND ?= json
CHUNK ?= 5000
# Local DSN (host=localhost). For docker-internal, use db service host.
PG_DSN ?= postgresql+psycopg2://reportuser:reportpass@localhost:5432/reportdb

# Docker
DOCKER_COMPOSE ?= docker compose
APP_SERVICE ?= app
DB_SERVICE ?= db

.PHONY: help venv install up up-all down logs-db app-shell db-psql \
        load-json load-fake load-json-docker \
        clean clean-pycache clean-venv \
        bot-local bot-docker logs-app \
        adminer-up adminer-down adminer-logs \
        clean-volumes rebuild recreate recreate-all

help:
	@echo "Commands:"
	@echo "  make venv                 - Create venv in .venv"
	@echo "  make install              - Install requirements into venv"
	@echo "  make up                   - Start only Postgres (db) via docker compose"
	@echo "  make up-all               - Start db and app services"
	@echo "  make down                 - Stop all services"
	@echo "  make logs-db              - Tail Postgres logs"
	@echo "  make app-shell            - Shell into app container"
	@echo "  make db-psql              - psql into db container"
	@echo "  make load-json            - Upsert $(JSON) into Postgres (local host)"
	@echo "  make load-fake            - Upsert generated fake data into Postgres"
	@echo "  make load-json-docker     - Upsert JSON from inside app container"
	@echo "  make bot-local            - Run Telegram bot locally (venv)"
	@echo "  make bot-docker           - Run bot via docker-compose (app service)"
	@echo "  make logs-app             - Tail app service logs"
	@echo "  make adminer-up           - Start Adminer (DB UI) on http://localhost:8080"
	@echo "  make adminer-down         - Stop Adminer"
	@echo "  make adminer-logs         - Tail Adminer logs"
	@echo "  make clean                - Remove caches and temporary files"
	@echo "  make clean-volumes        - Stop and remove containers and volumes"
	@echo "  make rebuild              - Force rebuild images (no cache)"
	@echo "  make recreate             - Rebuild containers from scratch and start all services"
	@echo "  make recreate-all         - Recreate and then load JSON into Postgres"

venv:
	$(PY) -m venv $(VENVDIR)

install: venv
	$(PIPV) install -U pip
	$(PIPV) install -r requirements.txt

# --- Docker lifecycle ---
up:
	$(DOCKER_COMPOSE) up -d $(DB_SERVICE)

up-all:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

logs-db:
	$(DOCKER_COMPOSE) logs -f $(DB_SERVICE)

app-shell:
	$(DOCKER_COMPOSE) exec $(APP_SERVICE) bash

db-psql:
	$(DOCKER_COMPOSE) exec -e PGPASSWORD=reportpass $(DB_SERVICE) psql -U reportuser -d reportdb

# --- Data loading (Upsert by order_id) ---
load-json: install
	$(PYTHON) load_to_pg.py --backend $(BACKEND) --json-path $(JSON) --pg-dsn "$(PG_DSN)" --table $(TABLE) --chunk-size $(CHUNK)

load-fake: install
	$(PYTHON) load_to_pg.py --backend fake --pg-dsn "$(PG_DSN)" --table $(TABLE) --chunk-size $(CHUNK)

# Uses docker-internal networking and env PG_DSN from docker-compose if available
load-json-docker:
	$(DOCKER_COMPOSE) exec $(APP_SERVICE) python load_to_pg.py --backend json --json-path /app/$(JSON) --pg-dsn "$${PG_DSN:-postgresql+psycopg2://reportuser:reportpass@db:5432/reportdb}" --table $(TABLE) --chunk-size $(CHUNK)

# --- Bot ---
bot-local: install
	$(PYTHON) bot.py

bot-docker:
	$(DOCKER_COMPOSE) up -d $(DB_SERVICE)
	$(DOCKER_COMPOSE) up -d $(APP_SERVICE)

logs-app:
	$(DOCKER_COMPOSE) logs -f $(APP_SERVICE)

adminer-up:
	$(DOCKER_COMPOSE) up -d adminer

adminer-down:
	$(DOCKER_COMPOSE) stop adminer

adminer-logs:
	$(DOCKER_COMPOSE) logs -f adminer

# --- Cleanup ---
clean: clean-pycache

clean-pycache:
	find . -type d -name '__pycache__' -exec rm -rf {} + || true
	find . -type f -name '*.pyc' -delete || true
	find . -type f -name '*.pyo' -delete || true

clean-venv:
	rm -rf $(VENVDIR)

# --- Recreate / Rebuild ---
clean-volumes:
	$(DOCKER_COMPOSE) down -v --remove-orphans || true
	$(DOCKER_COMPOSE) rm -fsv || true

rebuild:
	$(DOCKER_COMPOSE) build --no-cache

recreate: rebuild up-all
