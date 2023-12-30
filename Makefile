test:
	@./go.test.sh
.PHONY: test

coverage:
	@./go.coverage.sh
.PHONY: coverage

test_fast:
	go test ./...
.PHONY: test_fast

tidy:
	go mod tidy
.PHONY: tidy
