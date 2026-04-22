# Solar Analytics — common ops targets.
#
# Run "make help" to see all targets.
#
# Works with GNU Make on Linux/macOS and with `make` inside Git Bash on Windows.

COMPOSE       ?= docker compose
COMPOSE_DEV   := $(COMPOSE) -f docker-compose.yml
COMPOSE_PROD  := $(COMPOSE) -f docker-compose.yml -f docker-compose.prod.yml
COMPOSE_RDS   := $(COMPOSE) -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.rds.yml

BACKUP_DIR    ?= ./backups
BACKUP_FILE   ?=

.PHONY: help
help:
	@echo "Solar Analytics — available targets:"
	@echo ""
	@echo "  make run             Build + start the full dev stack (http://localhost:8080)"
	@echo "  make up              Start the dev stack without rebuilding"
	@echo "  make down            Stop the dev stack"
	@echo "  make logs            Tail logs from all services"
	@echo "  make ps              Show running services"
	@echo ""
	@echo "  make prod            Build + start the production stack (local DB)"
	@echo "  make rds             Build + start the production stack (external DB / RDS)"
	@echo ""
	@echo "  make migrate         Apply safe SQL migrations (sql/*)"
	@echo "  make migrate-status  Show pending + applied migrations"
	@echo "  make migrate-manual FILE=010_timestamps_to_native.sql"
	@echo "                       Apply a risky manual migration (requires backup)"
	@echo ""
	@echo "  make backup          Run pg_dump into \$$BACKUP_DIR (default ./backups)"
	@echo "  make restore FILE=<dump>   Restore a dump into the running DB"
	@echo ""
	@echo "  make shell           Open a shell in the backend container"
	@echo "  make psql            Open psql against the DB container"
	@echo "  make clean           Remove build artifacts (keeps volumes)"
	@echo "  make nuke            DROP everything, including DB volumes (destructive!)"

# ── Dev stack ──────────────────────────────────────────────────────────────
.PHONY: run up down logs ps
run:
	$(COMPOSE_DEV) up -d --build
	@echo ""
	@echo "UI: http://localhost:$${HTTP_PORT:-8080}"

up:
	$(COMPOSE_DEV) up -d

down:
	$(COMPOSE_DEV) down

logs:
	$(COMPOSE_DEV) logs -f --tail=200

ps:
	$(COMPOSE_DEV) ps

# ── Prod stacks ────────────────────────────────────────────────────────────
.PHONY: prod rds
prod:
	$(COMPOSE_PROD) up -d --build

rds:
	$(COMPOSE_RDS) up -d --build

# ── Migrations ─────────────────────────────────────────────────────────────
.PHONY: migrate migrate-status migrate-manual
migrate:
	$(COMPOSE_DEV) exec backend python -m migrations.runner auto

migrate-status:
	$(COMPOSE_DEV) exec backend python -m migrations.runner status

migrate-manual:
	@if [ -z "$(FILE)" ]; then echo "Usage: make migrate-manual FILE=xxx.sql"; exit 2; fi
	$(COMPOSE_DEV) exec backend python -m migrations.runner manual --file $(FILE)

# ── Backup / restore ───────────────────────────────────────────────────────
.PHONY: backup restore
backup:
	@mkdir -p $(BACKUP_DIR)
	$(COMPOSE_DEV) exec -T db sh -c 'pg_dump -U $$POSTGRES_USER -F c -Z 9 $$POSTGRES_DB' \
	  > $(BACKUP_DIR)/solar_$$(date +%Y%m%d_%H%M%S).dump
	@ls -lh $(BACKUP_DIR) | tail -n +2

restore:
	@if [ -z "$(FILE)" ]; then echo "Usage: make restore FILE=./backups/solar_XXXX.dump"; exit 2; fi
	$(COMPOSE_DEV) exec -T db sh -c 'pg_restore --clean --if-exists --no-owner --no-privileges \
	  -U $$POSTGRES_USER -d $$POSTGRES_DB' < $(FILE)

# ── Convenience ────────────────────────────────────────────────────────────
.PHONY: shell psql clean nuke
shell:
	$(COMPOSE_DEV) exec backend /bin/bash

psql:
	$(COMPOSE_DEV) exec db sh -c 'psql -U $$POSTGRES_USER $$POSTGRES_DB'

clean:
	$(COMPOSE_DEV) down --remove-orphans

nuke:
	$(COMPOSE_DEV) down -v --remove-orphans
