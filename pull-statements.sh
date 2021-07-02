#!/bin/sh

#set -e

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

go build cmd/divyield/main.go

urls="$(cat urls.csv | cut -f2)"

for i in $urls; do
    echo $i
    ./main.exe pull-statements -directory="statements" "$i"
done