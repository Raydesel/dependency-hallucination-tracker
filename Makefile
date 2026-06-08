PYTHON  ?= .venv/bin/python
LOG_DIR := logs
PID_DIR := .run

.PHONY: help up down ps wait-kafka topics smoke \
        poller parser validator sink \
        pipeline stop logs start restart clean

help:
	@echo "Dependency Hallucination Tracker — make targets"
	@echo ""
	@echo "  Infrastructure:"
	@echo "    make up          Start all Docker services (kafka, redis, postgres, grafana, ui)"
	@echo "    make down        Stop Docker services (keeps volumes/data)"
	@echo "    make ps          Show container status"
	@echo "    make clean       Stop services AND delete data volumes"
	@echo ""
	@echo "  Setup:"
	@echo "    make topics      Create Kafka topics (waits for Kafka to be healthy)"
	@echo "    make smoke       Run the produce/consume smoke test"
	@echo ""
	@echo "  Pipeline (background):"
	@echo "    make start       up + topics + pipeline (one command, full stack)"
	@echo "    make pipeline    Launch poller + parser + validator + sink in background"
	@echo "    make stop        Stop the background Python services"
	@echo "    make restart     stop + pipeline"
	@echo "    make logs        Tail all service logs"
	@echo ""
	@echo "  Individual services (foreground, for debugging):"
	@echo "    make poller | parser | validator | sink"

# ── Infrastructure ────────────────────────────────────────────────────────────

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

clean:
	docker compose down -v

# ── Setup ───────────────────────────────────────────────────────────────────

wait-kafka:
	@echo "Waiting for Kafka to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' dep-tracker-kafka 2>/dev/null)" = "healthy" ]; do \
		sleep 2; \
	done
	@echo "Kafka is healthy"

topics: wait-kafka
	$(PYTHON) infra/create_topics.py

smoke:
	$(PYTHON) infra/smoke_test.py

# ── Individual services (foreground) ──────────────────────────────────────────

poller:
	$(PYTHON) services/github-poller/poller.py

parser:
	$(PYTHON) consumers/commit_parser.py

validator:
	$(PYTHON) consumers/registry_validator.py

sink:
	$(PYTHON) consumers/sink_consumer.py

# ── Pipeline (background) ─────────────────────────────────────────────────────

pipeline:
	@mkdir -p $(LOG_DIR) $(PID_DIR)
	@echo "Starting pipeline services in background..."
	@nohup $(PYTHON) -u services/github-poller/poller.py    > $(LOG_DIR)/poller.log    2>&1 & echo $$! > $(PID_DIR)/poller.pid
	@nohup $(PYTHON) -u consumers/commit_parser.py          > $(LOG_DIR)/parser.log    2>&1 & echo $$! > $(PID_DIR)/parser.pid
	@nohup $(PYTHON) -u consumers/registry_validator.py     > $(LOG_DIR)/validator.log 2>&1 & echo $$! > $(PID_DIR)/validator.pid
	@nohup $(PYTHON) -u consumers/sink_consumer.py          > $(LOG_DIR)/sink.log      2>&1 & echo $$! > $(PID_DIR)/sink.pid
	@echo "Started. Logs in $(LOG_DIR)/, PIDs in $(PID_DIR)/"
	@echo "Tail them with: make logs"

stop:
	@echo "Stopping pipeline services..."
	@for f in $(PID_DIR)/*.pid; do \
		if [ -f "$$f" ]; then \
			pid=$$(cat "$$f"); \
			if kill "$$pid" 2>/dev/null; then echo "  stopped $$(basename $$f .pid) (pid $$pid)"; fi; \
			rm -f "$$f"; \
		fi; \
	done
	@echo "Done."

restart: stop pipeline

logs:
	tail -f $(LOG_DIR)/*.log

start: up wait-kafka topics pipeline
	@echo ""
	@echo "Full stack is up. Grafana: http://localhost:3000  |  Kafka UI: http://localhost:8080"
