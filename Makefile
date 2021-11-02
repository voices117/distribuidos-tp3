docker-compose.yml: service_config.py
	python compose_builder.py > docker-compose.yml

run: docker-compose.yml
	docker-compose up --remove-orphans

image:
	docker build -t tp2-client .

client:
	docker run --rm -e "LINES_PER_CHUNK=${LINES_PER_CHUNK}" \
	                -e "NUM_CHUNKS=${NUM_CHUNKS}" \
	                -e "RABBITMQ_ADDRESS=rabbitmq" \
					-v "$(shell pwd)/data:/app/data" \
					--network tp2_default \
					tp2-client client.py

.PHONY: client image run
