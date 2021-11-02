docker-compose.yml: service_config.py
	python compose_builder.py > docker-compose.yml

run: docker-compose.yml
	docker compose up --remove-orphans
