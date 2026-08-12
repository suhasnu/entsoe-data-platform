.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

up: ## Start local infrastructure
	docker compose up -d

down: ## Stop local infrastructure
	docker compose down

migrate: ## Apply SQL migrations in order
	@for f in migrations/*.sql; do echo "applying $$f"; \
	  docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -f - < $$f; done

test: ## Run tests
	pytest

lint: ## Lint and type check
	ruff check . && mypy src

.PHONY: help up down migrate test lint