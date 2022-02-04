#!/bin/sh

set -eux;

./build-go.sh \
	-o div/build/div \
	szakszon.com/divyield/div/cmd/div

docker build -t div div/

netip=$(docker network inspect -f '{{range .IPAM.Config}}{{.Subnet}}{{end}}' divyield)
vip=$(echo $netip | sed -E 's;[^.]+/(.*);100;g')

docker run --rm -it -e VIP=$vip div /bin/bash

