.PHONY: build
build:
	go build -trimpath -o go-sysbench cmd/main.go

.PHONY: lint
lint:
	golangci-lint run -v
	go vet -vettool=$HOME/go/bin/zagane ./...

.PHONY: test
test:
	go test -v ./...
