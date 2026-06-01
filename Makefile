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
