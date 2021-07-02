#!/bin/sh

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

go build cmd/divyield/main.go


exch="$(ls statements | sed 's#/##')"

for i in $exch; do
    echo $i
    symbols="$(ls statements/$i | sed 's#/##')"
    ./main.exe bargain -directory="statements/$i" $symbols
    echo
done
