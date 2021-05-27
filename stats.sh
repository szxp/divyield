#!/bin/sh

#set -x


# https://www.multpl.com/s-p-500-dividend-yield
sp500DivYield=1.39

divYieldFwdMin=$(echo $sp500DivYield 1.5 | awk '{print $1 * $2}')
divYieldFwdMax=$(echo $sp500DivYield 5.0 | awk '{print $1 * $2}')

tickers="$(cat tickers.csv | cut -d',' -f1 | tr '\r\n' ' ' | tr '\n', ' ')"

go build cmd/divyield/main.go && \
    ./main.exe \
    stats \
    -dividend-yield-forward-min=$divYieldFwdMin \
    -dividend-yield-forward-max=$divYieldFwdMax \
    -dividend-yield-roi-min=9.0 \
    -dgr5y-above-inflation \
    -gordon-roi-min=10.0 \
    -gordon-growth-rate-min=3.0 \
    -gordon-growth-rate-max=5.0 \
    -no-cut-dividend=true \
    -no-declining-dgr=true \
    -start-date=2010-01-01 \
    $@
    

