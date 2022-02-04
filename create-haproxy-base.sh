#!/bin/sh

set -eux

docker build -t haproxy:latest-divyield -f haproxy/Dockerfile-base haproxy/
