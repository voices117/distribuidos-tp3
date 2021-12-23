docker-compose.yml: service_config.py compose_builder.py
	python3 monitor_config_builder.py
	python compose_builder.py > docker-compose.yml

run: docker-compose.yml
	@COMPOSE_PARALLEL_LIMIT=30 docker-compose up --remove-orphans

image:
	docker build -t tp3-client .

client:
	docker run --rm -e "LINES_PER_CHUNK=${LINES_PER_CHUNK}" \
	                -e "NUM_CHUNKS=${NUM_CHUNKS}" \
	                -e "CORRELATION_ID=${CORRELATION_ID}" \
	                -e "WORKER_ID=0" \
	                -e "WORKER_TASK=client" \
	                -e "RABBITMQ_ADDRESS=rabbitmq" \
					-v "$(shell pwd):/app" \
					--network $(shell basename $(CURDIR) | sed 's/-//')_default \
					tp3-client client.py

test-kill-joiner:
	rm -rf killer_conf
	cp -r tests/kill_joiner ./killer_conf

.PHONY: client image run
