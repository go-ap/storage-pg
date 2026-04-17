SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

GO ?= go
TEST := $(GO) test
TEST_FLAGS ?= -v -tags conformance
TEST_TARGET ?= .
GO111MODULE = on
PROJECT_NAME := $(shell basename $(PWD))

.PHONY: test coverage clean download container

download: go.sum

go.sum: go.mod
	$(GO) mod tidy

test: go.sum clean
	$(TEST) $(TEST_FLAGS) -cover $(TEST_TARGET) -json | $(GO) tool tparse -all

coverage: go.sum clean
	@mkdir ./_coverage
	$(TEST) $(TEST_FLAGS) -covermode=count -args -test.gocoverdir="$(PWD)/_coverage" $(TEST_TARGET) > /dev/null || true
	$(GO) tool covdata percent -i=./_coverage/ -o $(PROJECT_NAME).coverprofile
	@$(RM) -r ./_coverage

clean:
	@$(RM) -r ./_coverage
	@$(RM) -v *.coverprofile
	@$(RM) -v tests.json

image:
	./images/image.sh

container: image
	podman run --rm --rmi --replace --name storage_pg \
		-e POSTGRES_USER=storage \
		-e POSTGRES_PASSWORD=storage \
		-e POSTGRES_HOST_AUTH_METHOD=trust \
		-p 5432:5432 \
		storage-pg
