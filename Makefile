export REPOSITORY=flyteplugins
include boilerplate/lyft/golang_test_targets/Makefile

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

generate: download_tooling
	@go generate ./...

clean:
	rm -rf bin

.PHONY: linux_compile
linux_compile:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o ./artifacts/flyte-copilot ./go/cmd/data/main.go

.PHONY: compile
compile:
	mkdir -p ./artifacts
	go build -o ./artifacts/flyte-copilot ./go/cmd/data/main.go

cross_compile:
	@glide install
	@mkdir -p ./artifacts/cross
	GOOS=linux GOARCH=amd64 go build -o ./artifacts/flyte-copilot ./go/cmd/data/main.go
