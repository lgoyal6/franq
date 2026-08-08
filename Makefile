BIN := bin

.PHONY: build test race bench crashtest vet clean

build:
	go build -o $(BIN)/franqd ./cmd/franqd
	go build -o $(BIN)/igor ./cmd/igor

test:
	go test ./...

race:
	go test -race ./...

bench:
	go test -bench=Commit -benchtime=2s -run='^$$' ./internal/wal

crashtest:
	go test -v -timeout 300s -run 'TestCrash|TestFailpoint' ./test/crash

vet:
	go vet ./...

clean:
	rm -rf $(BIN)
