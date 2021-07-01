#!/bin/sh

#set -e

function trap_ctrlc () {
    echo "Ctrl-C caught...performing clean up"
    echo "Doing cleanup"
    exit 2
}
 
trap "trap_ctrlc" 2

dirs="$(cat dirs.csv)"
for i in $dirs; do
    exc="$(echo $i | cut -d"/" -f1)"
    sym="$(echo $i | cut -d"/" -f2)"

    if [ -d "statements/$sym" ]; then
        #echo $exc $sym
        mv "statements/$sym" "statements/$exc/$sym"
    fi
done
