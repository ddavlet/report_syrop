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

.PHONY: help venv install up down clean clean-venv clean-volumes rebuild recreate bot data-loader setup-db \
compose-up compose-down compose-logs compose-ps compose-app compose-data-loader compose-db compose-adminer init-db-docker stop

help:
	@echo "Commands:"
	@echo "  make venv                 - Create virtual environment"
	@echo "  make install              - Install requirements into venv"
	@echo "  make up                   - Build images and start all services (detached)"
	@echo "  make down                 - Stop all services"
	@echo "  make bot                  - Run Telegram bot locally"
	@echo "  make data-loader          - Run data loader service locally"
	@echo "  make setup-db             - Setup database tables (requires PG_DSN)"
	@echo "  make clean                - Remove caches and temporary files"
	@echo "  make clean-venv           - Remove virtual environment"
	@echo "  make clean-volumes        - Stop and remove containers and volumes"
	@echo "  make rebuild              - Force rebuild images (no cache)"
	@echo "  make recreate             - Rebuild containers and start services"
	@echo "  make compose-logs         - Tail logs for all Docker services"
	@echo "  make compose-ps           - Show Docker services status"
	@echo "  make init-db-docker       - Initialize DB schema inside the app container"

venv:
	$(PY) -m venv $(VENVDIR)

install: venv
	$(PIPV) install -U pip
	$(PIPV) install -r requirements.txt

# --- Docker lifecycle ---
up: build
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

build:
	$(DOCKER_COMPOSE) build

stop:
	$(DOCKER_COMPOSE) stop

# --- Docker Compose helpers ---
compose-logs:
	$(DOCKER_COMPOSE) logs -f

compose-ps:
	$(DOCKER_COMPOSE) ps


# --- Initialize DB schema inside container ---
init-db-docker:
	$(DOCKER_COMPOSE) exec app sh -lc 'python scripts/setup_database.py --pg-dsn "$$PG_DSN"'

# --- Services ---
bot: install
	$(PYTHON) -m src.bot

data-loader: install
	$(PYTHON) -m src.data_loader_service

load-data:
	curl -X POST http://localhost:5500/update \
     -H "Content-Type: application/json" \
     -d @data/123.json

# --- Database ---
setup-db: install
	@echo "Usage: make setup-db PG_DSN='postgresql://user:pass@host:port/db'"
	@if [ -z "$(PG_DSN)" ]; then \
		echo "Error: PG_DSN environment variable is required"; \
		echo "Example: make setup-db PG_DSN='postgresql://user:pass@localhost:5432/sales_db'"; \
		exit 1; \
	fi
	$(PYTHON) scripts/setup_database.py --pg-dsn "$(PG_DSN)"


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


# --- Reports ---
report-sales-with-items:
	$(DOCKER_COMPOSE) exec app sh -lc 'python scripts/return_sales.py --start-date "2024-01-01" --limit 1'

deploy:
	rsync -avz --delete --exclude 'certs' --exclude 'www' --exclude '.git' --exclude '.venv' --exclude '.cursor' --exclude '.DS_Store' --exclude '__pycache__' ./ ddavlet@10.100.0.31:/home/ddavlet/reports_app
