generate:
	go generate ./...

pg-up:
	docker run --name otelbench-pg -e POSTGRES_DB=otelbench -e POSTGRES_PASSWORD=otelbench -e POSTGRES_USER=otelbench -p 127.0.0.1:5432:5432 -d postgres:14
pg-down:
	docker rm -f otelbench-pg
pg-migrate:
	atlas migrate apply --env dev
pg-connect:
	psql postgres://otelbench:otelbench@localhost:5432/otelbench

# re-generate init migration
atlas-reset:
	rm -f migrations/*
	go generate ./ent
	atlas migrate --env dev diff init

db-reset: generate atlas-reset
