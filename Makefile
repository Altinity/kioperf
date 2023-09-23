.PHONY: all
all: kioperf

kioperf: cmd/*.go pkg/*/*.go Makefile go.mod go.sum
	go build cmd/kioperf.go

fmt: cmd/*.go pkg/*/*.go
	go fmt cmd/*.go
	go fmt pkg/*/*.go

test: .PHONY
	test/smoke.sh

tidy:
	go mod tidy

dependencies:
	go mod download

clean:
	rm -f kioperf
	rm -rf test
