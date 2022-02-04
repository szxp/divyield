#!/bin/sh

set -eux

SCRIPT=$(readlink -f "$0")
SCRIPTDIR=$(dirname "$SCRIPT")
cd $SCRIPTDIR

APPROOT=/app

# Using Docker Desktop on Windows 10
# in Hyper-V mode
# you have to add that $PWD repo root folder 
# to "Resources" -> "File sharing"
# https://docs.docker.com/desktop/windows/#file-sharing 
docker run \
	--rm \
	-v "$PWD":$APPROOT \
	-w $APPROOT \
	protoc:latest \
	protoc \
		--go_out="." \
		--go_opt=paths=source_relative \
	    --go-grpc_out="." \
		--go-grpc_opt=paths=source_relative \
	    paper.proto
