# divyield

I have read a book about how to select stocks that pay enough dividends to build a good portfolio. This project helps me find those stocks. The financial data are provided by the IEX Cloud data provider.

## Version 1

The first version is a CLI tool that downloads financial data from IEX Cloud, caches the data in a PostgreSQL database and finds good enough stocks.

Create the PostgreSQL database using the `postgres/scripts/createdb.sql` file.

Pull financial data (such as splits, prices and dividends):

```
divyield pull
```

Find good enough stocks:
```
sh stats.sh
```

Example stats.sh output:
```
  Company                                 Exchange                           Dividend fwd  Yield fwd    GGR    MR% date    MR%  DGR-1y  DGR-2y  DGR-3y  DGR-4y
  CQP - Cheniere Energy Partners LP       NYSE MKT LLC                               2.72      6.42%  3.58%  2021-11-04  2.26%   5.99%   8.22%  14.36%  10.83%
  OKE - Oneok Inc.                        NEW YORK STOCK EXCHANGE INC.               3.74      5.95%  4.05%  2020-01-24  2.19%   5.95%   7.36%  11.20%  11.04%
  PNW - Pinnacle West Capital Corp.       NEW YORK STOCK EXCHANGE INC.               3.32      5.14%  4.86%  2020-10-30  6.07%   6.09%   6.10%   6.10%   5.86%
  SGU - Star Group L.P.                   NEW YORK STOCK EXCHANGE INC.               0.57      5.08%  4.92%  2021-04-30  7.55%   6.09%   6.29%   6.50%   6.74%
  UVV - Universal Corp.                   NEW YORK STOCK EXCHANGE INC.               3.12      6.28%  3.72%  2021-07-09  1.30%   1.32%   8.49%  12.31%   9.61%
  WU - Western Union Company              NEW YORK STOCK EXCHANGE INC.               0.94      5.07%  4.93%  2021-03-16  4.44%  12.50%   8.82%   8.74%   8.90%

Number of companies:       6
Start date:                2016-01-01
Inflation (HUN current):   6.50%, 2021. október, KSH
S&P 500 dividend yield:    1.29%, 10:16 AM EST, Fri Nov 26
Dividend yield total min:  9.00%
Dividend yield fwd min:    1.94%
Dividend yield fwd max:    6.45%
GGR ROI:                   10.00%
GGR min:                   2.00%
GGR max:                   5.00%
DGRAvg min:                5.00%
No cut dividend
No declining DGR
DGR yearly
```



