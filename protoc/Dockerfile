# Base image for compiling proto files.

FROM golang:1.17.6

RUN set -eux; \
	apt update; \
	apt install -y protobuf-compiler=3.12.4-1; \
	protoc --version; \
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1; \
	protoc-gen-go --version; \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0; \
	protoc-gen-go-grpc --version;

