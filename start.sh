#!/bin/sh

set -eux;

KEEPALIVED_URL="https://www.keepalived.org/software/keepalived-2.2.4.tar.gz"
KEEPALIVED_MD5SUM="7097ba70a7c6c46c9e478d16af390a19"

if [ ! -f haproxy/keepalived.tar.gz ]; then
	curl -sSL -o haproxy/keepalived.tar.gz "$KEEPALIVED_URL"
	echo "$KEEPALIVED_MD5SUM *haproxy/keepalived.tar.gz" | md5sum -c 
fi


netip=$(docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}}{{end}}' divyield)
export VIP=$(echo $netip | sed -E 's;[^.]+/(.*);100/\1;g')


./paper/compile-proto.sh

./build-go.sh \
	-o paper/build/paper \
	szakszon.com/divyield/paper/cmd/paper

docker-compose up --build --no-start
docker-compose start


