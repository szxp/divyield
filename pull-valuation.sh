#!/bin/sh

#set -e

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

go build cmd/divyield/main.go && \
    ./main.exe pull-valuation -directory="statements" banks-eu.csv

