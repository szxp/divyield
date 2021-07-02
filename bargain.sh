#!/bin/sh

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

go build cmd/divyield/main.go

symbols="$(cat urls.csv | cut -f1)"

exch="$(find statements -maxdepth 1 -type d -printf '%f ')"

for i in $exch; do
    echo $i
    ./main.exe bargain -directory="statements/$i" $symbols
    echo
done
