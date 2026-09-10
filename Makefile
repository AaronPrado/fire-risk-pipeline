.PHONY: all lint format test-pipeline test-chatbot eval run

all: lint format test-pipeline test-chatbot

lint:
	ruff check .

format:
	ruff format .

# Requiere env: firerisk (Python 3.11)
test-pipeline:
	pytest tests/ -v

# Requiere env: firerisk-chatbot (Python 3.12)
test-chatbot:
	pytest chatbot/tests/ -v

# Requiere env: firerisk-chatbot (Python 3.12)
eval:
	python -m chatbot.eval.run_eval

# Requiere env: firerisk-chatbot (Python 3.12)
run:
	python -m chatbot.app

PYTHON ?= python3
VENV := .venv-demo

.PHONY: demo
demo:  ## Levanta el pipeline sobre MinIO con datos de muestra, sin credenciales AWS
	docker compose -p fire-risk-demo -f docker-compose.demo.yml up -d
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements-demo.txt
	AWS_ENDPOINT_URL=http://localhost:9000 \
	AWS_ACCESS_KEY_ID=demo AWS_SECRET_ACCESS_KEY=demo12345 \
	AWS_DEFAULT_REGION=eu-west-1 S3_BUCKET=fire-risk-demo \
	START_DATE=$$(date -d '-30 days' +%F 2>/dev/null || date -v-30d +%F) \
	END_DATE=$$(date +%F) \
	$(VENV)/bin/python scripts/backfill.py

.PHONY: demo-down
demo-down:  ## Para la demo y borra el entorno temporal
	docker compose -p fire-risk-demo -f docker-compose.demo.yml down -v
	rm -rf $(VENV)
