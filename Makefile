include dbt_project/dbt.env

# COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

TARGET_MAX_CHAR_NUM=20

## Show help with `make help`
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			helpDir = match(lastLine, /@`([^`]+)`/); \
			if (helpDir) { \
				helpDir = substr(lastLine, RSTART + 2, RLENGTH - 3); \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET} in `%s`\n", helpCommand, helpMessage, helpDir; \
			} else { \
				printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
			} \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)


.PHONY: dbt-run
## Activate the virtual environment and source dbt.env
dbt-run:
	@if [ -d capstone-dbt_2/dbt_venv ]; then rm -rf capstone-dbt_2/dbt_venv; fi
	python3 -m venv capstone-dbt_2/dbt_venv
	@echo "Virtual environment created."
	@. capstone-dbt_2/dbt_venv/bin/activate && \
	pip install --upgrade pip && \
	pip install -r capstone-dbt_2/dbt-requirements.txt && \
	source capstone-dbt_2/dbt.env && \
	cd capstone-dbt_2 && \
	exec /bin/bash
