update s_agco.dividend 
    set 
    frequency = 0, 
    payment_type = 'Special' 
    where 
    ex_date = '2021-05-07' and 
    amount = 4;

call update_dividend_adj('s_agco');
call update_price_adj('s_agco');


update s_cswc.dividend 
    set 
    frequency = 0, 
    payment_type = 'Special' 
    where 
    ex_date = '2019-12-19' and 
    amount = 0.75;
    
delete from s_cswc.dividend 
    where
    ex_date = '2018-06-25' and 
    amount = 0.6;

insert into s_cswc.dividend 
    (id, ex_date, symbol, amount, currency, frequency, payment_type)
    values
    (1358873 , '2018-06-25', 'CSWC', 0.10, 'USD', 4, 'Cash'),
    (1358874 , '2018-06-25', 'CSWC', 0.50, 'USD', 0, 'Special');

update s_cswc.dividend 
    set 
    frequency = 0, 
    payment_type = 'Special' 
    where 
    ex_date = '2017-03-13' and 
    amount = 0.26;
    
call update_dividend_adj('s_cswc');
call update_price_adj('s_cswc');



update s_otex.dividend 
    set 
    amount = 0.2008, 
    currency = 'USD' 
    where 
    ex_date = '2020-12-03';

call update_dividend_adj('s_otex');
call update_price_adj('s_otex');



delete from s_istr.dividend 
    where ex_date = '2021-03-26' 
    and amount = 0.18;

call update_dividend_adj('s_istr');
call update_price_adj('s_istr');
