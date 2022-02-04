#!/bin/sh

./create-haproxy-base.sh

./create-protoc-base.sh


echo "Create volume for PostgreSQL data"
docker volume create --name=pgdata

net=divyield
echo "Create network"
docker network inspect $net >/dev/null 2>&1 || \
    docker network create -d bridge $net >/dev/null 2>&1 

netip=$(docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}}{{end}}' divyield)
echo "$net $netip"


