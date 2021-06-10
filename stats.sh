#!/bin/sh

#set -x

go build cmd/divyield/main.go && \
    ./main.exe \
    stats \
    -dividend-yield-forward-sp500-min=1.5 \
    -dividend-yield-forward-sp500-max=4.0 \
    -dividend-yield-roi-min=9.0 \
    -dgr5y-min=4.0 \
    -gordon-roi-min=10.0 \
    -gordon-growth-rate-min=2.0 \
    -gordon-growth-rate-max=5.0 \
    -no-cut-dividend=true \
    -no-declining-dgr=true \
    -start-date=-5y \
    $@
    

