.PHONY: dev logs stop clean build build-emr_eks_container_example_dag-image build-aws build-google-cloud build-run docs
.PHONY: restart restart-all run-tests run-static-checks run-mypy run-local-lineage-server test-rc-deps shell help

ASTRO_PROVIDER_VERSION ?= "dev"

# If the first argument is "run"...
ifeq (run-mypy,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  ifndef RUN_ARGS
  RUN_ARGS := .
  endif
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

ASTRO_RUNTIME_IMAGE_NAME = "quay.io/astronomer/astro-runtime:8.2.0-base"

dev: ## Create a development Environment using `docker compose` file.
	IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) docker compose -f dev/docker-compose.yaml up -d

logs: ## View logs of the all the containers
	docker compose -f dev/docker-compose.yaml logs --follow

stop: ## Stop all the containers
	docker compose -f dev/docker-compose.yaml down

clean: ## Remove all the containers along with volumes
	docker compose -f dev/docker-compose.yaml down  --volumes --remove-orphans
	rm -rf dev/logs

build: ## Build the Docker image (ignoring cache)
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t airflow-provider-fivetran-async-dev:latest --no-cache

build-run: ## Build the Docker Image & then run the containers
	IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) docker compose -f dev/docker-compose.yaml up --build -d

restart: ## Restart Triggerer, Scheduler and Worker containers
	docker compose -f dev/docker-compose.yaml restart airflow-triggerer airflow-scheduler airflow-worker

restart-all: ## Restart all the containers
	docker compose -f dev/docker-compose.yaml restart

run-tests: ## Run CI tests
	docker build --build-arg IMAGE_NAME=$(ASTRO_RUNTIME_IMAGE_NAME) -f dev/Dockerfile . -t airflow-provider-fivetran-async-dev
	docker run -v `pwd`:/usr/local/airflow/airflow_provider_fivetran_async -v `pwd`/dev/.cache:/home/astro/.cache \
        -w /usr/local/airflow/airflow_provider_fivetran_async \
		--rm -it airflow-provider-fivetran-async-dev -- pytest --cov astronomer --cov-report=term-missing tests

shell:  ## Runs a shell within a container (Allows interactive session)
	docker compose -f dev/docker-compose.yaml run --rm airflow-scheduler bash

help: ## Prints this message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-41s\033[0m %s\n", $$1, $$2}'
