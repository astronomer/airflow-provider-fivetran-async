.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Setup development environment
	python -m venv venv
	. venv/bin/activate && pip --no-cache-dir install ".[tests]"
	@echo "To activate the virtual environment, run:"
	@echo "source venv/bin/activate"

.PHONY: build-whl
build-whl: ## Build installable whl file
	rm -rf dev/include/*
	rm -rf dist/*
	mkdir -p dev/include
	uv build --wheel --sdist
	cp dist/* dev/include/

.PHONY: docker-run
docker-run: build-whl ## Runs local Airflow for testing
	@if ! lsof -i :8080 | grep LISTEN > /dev/null; then \
		cd dev && astro dev start --verbosity debug; \
	else \
		cd dev && astro dev restart --verbosity debug; \
	fi

.PHONY: docker-stop
docker-stop: ## Stop Docker container
	cd dev && astro dev stop
