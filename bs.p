set terminal png size 1920,1080;
set output imgfile;

set datafile columnheaders;
set datafile separator ';';

set xdata time; 
set timefmt "%Y-%m-%d";
#set xtics ("2021-03-30", "2020-12-30", "2020-09-29", "2020-06-29", "2020-03-30");
set xtics rotate by -20;
set format x "%Y-%m-%d"


set decimal locale;
set format y "%'.1f";

plot \
    dfile using 1:2 \
    with linespoints \
    lw 3 \
    lc 'green' \
    title 'Total Assets', \
    dfile using 1:3 \
    with linespoints \
    lw 3 \
    lc 'red' \
    title 'Total Liabilities', \
    dfile using 1:($2-$3) \
    with linespoints \
    lw 3 \
    lc 'royalblue' \
    title 'Total Equity';
