docker-compose.yml: config.py
	python3 compose_builder.py > docker-compose.yaml

run: docker-compose.yml
	docker-compose up --remove-orphans

.PHONY: run
