#!/bin/sh

#set -x

for i in `cat tickers.txt`; do
    if grep -q "^$i," tokens.csv; then
        echo "Found: $i"
    else
        echo "Not found: $i"
        echo "$i," >> tokens.csv
        output="$(node ../../../ws/bitbucket.org/ticker-iex/iex.js create-account "$i" 2>&1)"

        token="$(echo $output | sed -nr "s/.*(pk_[a-z0-9_]{32}).*/\1/p")"
        echo "TOKEN: $token"
        sed -i "s/^$i,/$i,$token/g" tokens.csv
    fi
done

