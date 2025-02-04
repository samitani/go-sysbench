.PHONY: build
build:
	go build -trimpath -o go-sysbench cmd/go-sysbench/main.go

.PHONY: lint
lint:
	golangci-lint run -v
	go vet -vettool=$HOME/go/bin/zagane ./...

.PHONY: test
test:
	go test -coverprofile=coverage.txt -covermode=atomic -v -race ./...
