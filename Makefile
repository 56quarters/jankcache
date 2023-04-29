USE_CGO := 0
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
GIT_VERSION := dev
DOCKER_VERSION := dev

default: build

.PHONY: build build-dist clean image image-dist lint test version

cmd/jankcache/jankcache:


build:
	env CGO_ENABLED=$(USE_CGO) go build -o cmd/jankcache/jankcache -ldflags="-X 'main.Branch=$(GIT_BRANCH)' -X 'main.Revision=$(GIT_REVISION)' -X 'main.Version=$(GIT_VERSION)'" cmd/jankcache/main.go

build-dist: version
build-dist: GIT_VERSION = $(shell cat VERSION)
build-dist: build

clean:
	rm -f jankcache
	rm -f VERSION

image: build
	docker build -t "jankcache:latest" -t "jankcache:$(DOCKER_VERSION)" .

image-dist: build-dist
image-dist: DOCKER_VERSION = $(shell cat VERSION)
image-dist: image

lint:
	golangci-lint run

setup:
	GO111MODULE=on go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.52.2

test:
	go test -tags netgo -timeout 5m -race -count 1 ./...

version:
	git describe --tags --abbrev=0 > VERSION
