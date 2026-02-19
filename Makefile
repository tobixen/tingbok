.PHONY: help install dev lint format test clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install the package (auto-detects uv, pipx, or pip)
	@if command -v uv >/dev/null 2>&1; then \
		echo "Installing with uv..."; \
		uv tool install .; \
	elif command -v pipx >/dev/null 2>&1; then \
		echo "Installing with pipx..."; \
		pipx install .; \
	else \
		echo "Tip: Install uv or pipx for isolated installs (pacman -S uv, apt install pipx, brew install uv)"; \
		echo "Falling back to pip install --user ..."; \
		PIP_BREAK_SYSTEM_PACKAGES=1 pip install --user .; \
	fi

dev:  ## Install with dev dependencies (editable)
	PIP_BREAK_SYSTEM_PACKAGES=1 pip install -e ".[dev]"

lint:  ## Run ruff linter and formatter check
	python -m ruff check src/ tests/
	python -m ruff format --check src/ tests/

format:  ## Auto-format code
	python -m ruff check --fix src/ tests/
	python -m ruff format src/ tests/

run:  ## Run the dev server using the local inventory-md SKOS cache
	TINGBOK_CACHE_DIR=$$HOME/.cache/inventory-md uvicorn tingbok.app:app --reload --port 5100

test:  ## Run tests
	python -m pytest

clean:  ## Remove build artifacts
	rm -rf dist/ build/ *.egg-info src/*.egg-info .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
