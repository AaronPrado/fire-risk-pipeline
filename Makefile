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

.PHONY: demo
demo:  ## Levanta el pipeline sobre MinIO con datos de muestra, sin credenciales AWS
	docker compose -f docker-compose.demo.yml up -d
	AWS_ENDPOINT_URL=http://localhost:9000 \
	AWS_ACCESS_KEY_ID=demo AWS_SECRET_ACCESS_KEY=demo12345 \
	AWS_DEFAULT_REGION=eu-west-1 S3_BUCKET=fire-risk-demo \
	python -m scripts.demo_seed
