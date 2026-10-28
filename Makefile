.DEFAULT_GOAL := help

help:
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

up: ## Start local infrastructure
	docker compose up -d

down: ## Stop local infrastructure
	docker compose down

stream-up: ## Start Kafka and the topic browser
	docker compose --profile streaming up -d

stream-down: ## Stop Kafka without touching Airflow
	docker compose --profile streaming down

stream-tail: ## Print whatever is on the load topic
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 --topic entsoe.load.raw \
	  --from-beginning --property print.key=true --max-messages 5

spark-hourly: ## Aggregate the load topic into hourly windows
	docker compose --profile streaming run --rm spark

migrate: ## Apply SQL migrations in order
	@for f in migrations/*.sql; do echo "applying $$f"; \
	  docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -f - < $$f; done

test: ## Run tests
	pytest

lint: ## Lint and type check
	ruff check . && mypy src

.PHONY: help up down stream-up stream-down stream-tail spark-hourly migrate test lint