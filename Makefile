test:
	@./go.test.sh
.PHONY: test

coverage:
	@./go.coverage.sh
.PHONY: coverage

test_fast:
	go test ./...

tidy:
	go mod tidy

yt-metric-bench:
	KO_DOCKER_REPO=cloud-registry.odkl.ru/dash/resource-dashboard/moc
