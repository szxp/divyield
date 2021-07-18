for i in `cat banks-eu.txt`; do 
    grep "^$i\s" urls-eu.csv >> banks-eu.csv
done
