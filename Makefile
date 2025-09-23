BINARY ?= hexatiles
DIST ?= dist

.PHONY: build test fmt tidy sample clean

build:
	go build -o $(BINARY) ./cmd/hexatiles

test:
	go test ./...

fmt:
	gofmt -w cmd internal

tidy:
	go mod tidy

sample: build
	mkdir -p $(DIST)
	@echo "Generating sample Parquet file..."
	./$(BINARY) sample --out $(DIST)/sample.parquet --count 5 --resolution 8
	./$(BINARY) build --in $(DIST)/sample.parquet --out $(DIST)/sample.pmtiles --keep-ndjson

.PHONY: release
release:
	goreleaser release --clean

clean:
	rm -rf $(DIST)
	rm -f $(BINARY)
