.PHONY: local-up local-down local-trail

data:
	mkdir data
	cp -a ./dynamo/. ./data/

local-up: data
	docker-compose up -d

local-down:
	docker-compose down

local-logs:
	docker-compose logs -f dynamodb

clean:
	git clean -fXd