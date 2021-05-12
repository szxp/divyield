#!/bin/sh

go build cmd/divyield/main.go && ./main.exe stats \
    -sp500-dividend-yield=1.38 \
    -gordon-growth-rate-min=3.0 \
    -gordon-growth-rate-max=5.0 \
    `cat tokens.csv | cut -d',' -f1 | tr '\n', ' '`

