export REPOSITORY=flyteplugins
include boilerplate/flyteorg/docker_build/Makefile
include boilerplate/flyteorg/golang_test_targets/Makefile

.PHONY: update_boilerplate
update_boilerplate:
	@boilerplate/update.sh

generate: download_tooling
	@go generate ./...
