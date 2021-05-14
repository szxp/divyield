#!/bin/sh

go build cmd/divyield/main.go && \
    ./main.exe \
    -no-cut-dividend \
    -start-date=2010-01-01 \
    stats \
    -sp500-dividend-yield=1.40 \
    -gordon-growth-rate-min=3.0 \
    -gordon-growth-rate-max=5.0 \
    `cat tokens.csv | cut -d',' -f1 | tr '\n', ' '`

