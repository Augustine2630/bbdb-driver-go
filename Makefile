.PHONY: proto/gen build test

proto/gen:
	buf generate

build:
	go build ./...

test:
	go test ./... -count=1

test/cover:
	go test ./... -count=1 -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1
