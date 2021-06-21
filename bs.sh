#!/bin/sh

#set -x

wdir="work/bs/us"
symbols="symbols-us-driping.txt"

prices()
{
    endTime="$(date +'%s')"
    startTime="$(($endTime-864000))" # 10 days ago
    symbol="$1"
    output="$2"
    u="https://query1.finance.yahoo.com/v7/finance/download/$symbol?period1=$startTime&period2=$endTime&interval=1d&events=history&includeAdjustedClose=true"
    curl -Ls -o "$output" "$u" 
}

go build cmd/divyield/main.go

for i in `cat $symbols`; do

    dfile="$wdir/$i.csv"

    if [ ! -f "$dfile" ]; then
        echo "$i: downloading"
        ./main.exe bs "$i" >"$dfile" 2>&1
        symbolYahoo="$(head -n1 "$dfile" | cut -d, -f1)"
        prices "$symbolYahoo" "$wdir/$i.prices.csv"
    else
        echo "$i: already downloaded"
    fi

    if [ -f "$wdir/0stop" ]; then
        echo "Stop"
        exit
    fi

done
        
#imgfile="$wdir/$i.png"
#gnuplot -e "dfile='$dfile'; imgfile='$imgfile'" bs.p
