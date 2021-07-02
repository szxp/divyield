#!/bin/sh

#set -x


# https://tools.mhinvest.com/mhichart

go build cmd/divyield/main.go && \
    ./main.exe \
    stats \
    -start-date=-5y \
    -no-cut-dividend=true \
    -no-declining-dgr=true \
    -dgr-avg-min=5.0 \
    -dgr-yearly=true \
    -dividend-yield-forward-sp500-min=1.5 \
    -dividend-yield-forward-sp500-max=5.0 \
    -dividend-yield-roi-min=9.0 \
    -gordon-roi-min=11.0 \
    -gordon-growth-rate-min=2.0 \
    -gordon-growth-rate-max=5.0 \
    $@ | \
    less -S
    

