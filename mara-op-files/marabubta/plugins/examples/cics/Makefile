# Marabunta - Licensed under the MIT License.
.PHONY: build clean test run

BINARY := marabunta-cics
CMD_DIR := ./cmd/marabunta-cics

build:
	go build -o $(BINARY) $(CMD_DIR)

clean:
	rm -f $(BINARY)

test:
	go test -v -race ./...

run: build
	./$(BINARY)

lint:
	golangci-lint run ./...
