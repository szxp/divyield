#!/bin/sh

#set -x

go build cmd/divyield/main.go && \
    ./main.exe \
    stats \
    -dividend-yield-forward-sp500-min=1.5 \
    -dividend-yield-forward-sp500-max=5.0 \
    -dividend-yield-roi-min=9.0 \
    -dgr5y-above-inflation \
    -gordon-roi-min=10.0 \
    -gordon-growth-rate-min=3.0 \
    -gordon-growth-rate-max=5.0 \
    -no-cut-dividend=true \
    -start-date=-5y \
    $@
    
#    -no-declining-dgr=true \

