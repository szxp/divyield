#!/bin/sh

#set -x


# https://www.multpl.com/s-p-500-dividend-yield
sp500DivYield=1.38

divYieldFwdMin=$(echo $sp500DivYield 1.5 | awk '{print $1 * $2}')
divYieldFwdMax=$(echo $sp500DivYield 5.0 | awk '{print $1 * $2}')

tickers="$(cat tickers.csv | cut -d',' -f1 | tr '\r\n' ' ' | tr '\n', ' ')"

go build cmd/divyield/main.go && \
    ./main.exe \
    -no-cut-dividend \
    -no-declining-dgr=false \
    -dividend-yield-forward-min=$divYieldFwdMin \
    -dividend-yield-forward-max=$divYieldFwdMax \
    -gordon-roi=10.0 \
    -gordon-growth-rate-min=3.0 \
    -gordon-growth-rate-max=5.0 \
    -startDate=2010-01-01 \
    stats \
    $tickers

