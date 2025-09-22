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
	@echo "(Skipping Parquet generator; please place a sample Parquet at $(DIST)/sample.parquet)"
	./$(BINARY) build --in $(DIST)/sample.parquet --out $(DIST)/sample.pmtiles --keep-ndjson

clean:
	rm -rf $(DIST)
	rm -f $(BINARY)
