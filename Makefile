VERSION=$(shell cat VERSION)
LDFLAGS=-ldflags "-X main.version=$(VERSION)"

.PHONY: build
build:
	go build $(LDFLAGS) -trimpath -o go-sysbench ./cmd/go-sysbench/...

.PHONY: lint
lint:
	golangci-lint run -v

.PHONY: test
test:
	go test -coverprofile=coverage.txt -covermode=atomic -v -race ./...
