#!/bin/sh

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

go build cmd/divyield/main.go

symbols="$(cat urls.csv | cut -f1)"

./main.exe bargain $symbols
