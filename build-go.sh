#!/bin/sh

set -eux

APPROOT=/go/app

# Using Docker Desktop on Windows 10
# in Hyper-V mode
# you have to add that $PWD repo root folder 
# to "Resources" -> "File sharing"
# https://docs.docker.com/desktop/windows/#file-sharing 
docker run \
	--rm \
	-v "$PWD":$APPROOT \
	-w $APPROOT \
	-e GOPATH=$APPROOT/build \
	-e GOCACHE=$APPROOT/build/cache/go-build \
	golang:1.17.6 \
	go build -v "$@"

