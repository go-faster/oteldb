test:
	@./go.test.sh
.PHONY: test

coverage:
	@./go.coverage.sh
.PHONY: coverage

test_fast:
	go test ./...
.PHONY: test_fast

scc:
	scc -z --exclude-dir _testdata --exclude-dir opentelemetry-collector-contrib
.PHONY: scc

tidy:
	go mod tidy
.PHONY: tidy
