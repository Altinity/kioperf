.PHONY: all
all: ioperf

ioperf: cmd/*.go pkg/*/*.go Makefile go.mod go.sum
	go build cmd/ioperf.go

fmt: cmd/*.go pkg/*/*.go
	go fmt cmd/*.go
	go fmt pkg/*/*.go

#test: .PHONY
#        go test internal/database/database_test.go -v

tidy:
	go mod tidy

dependencies:
	go mod download

clean:
	rm -f ioperf
	rm -rf test
