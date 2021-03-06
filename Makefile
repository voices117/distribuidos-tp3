docker-compose.yml: service_config.py compose_builder.py
	python3 monitor_config_builder.py
	python compose_builder.py > docker-compose.yml

run: docker-compose.yml
	@COMPOSE_PARALLEL_LIMIT=200 docker-compose up -d --remove-orphans rabbitmq
	@sleep 3
	@COMPOSE_PARALLEL_LIMIT=200 docker-compose up --remove-orphans

clean:
	rm -rf data_* logs killer_conf ./monitor/src/config*

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

case-1: clean
	python3 monitor_config_builder.py
	cp -r tests/case_1 ./killer_conf

case-2: clean
	python3 monitor_config_builder.py
	cp -r tests/case_2 ./killer_conf

case-3: clean
	python3 monitor_config_builder.py
	cp -r tests/case_3 ./killer_conf

.PHONY: client image run test-kill-joiner clean
