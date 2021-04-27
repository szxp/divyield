create or replace procedure public.update_dividend_adj(schema_name text)
language plpgsql
as $$
declare 
    r record;
    factor numeric; 
begin
    execute 'update ' || quote_ident(schema_name) || '.dividend set ' || 
        ' factor_adj = 1, amount_adj = amount';

    for r in execute 'select * from ' || 
        quote_ident(schema_name) || '.split order by ex_date desc'
    loop
        factor := 1.0 / (r.to_factor / r.from_factor);        
        execute 'update ' || quote_ident(schema_name) || '.dividend set ' || 
            ' factor_adj = factor_adj * ' || factor ||  
            ' where ex_date < ''' || r.ex_date || '''';
    end loop;

    execute 'update ' || quote_ident(schema_name) || '.dividend set ' || 
        ' factor_adj = round(factor_adj, 4) ' 
        ', amount_adj = round(amount * factor_adj, 4) ';

end $$;


create or replace procedure public.update_price_adj(schema_name text)
language plpgsql
as $$
declare 
    r record;
    factor numeric; 
begin
    execute 'update ' || quote_ident(schema_name) || '.price set ' || 
        ' factor_adj = 1, close_adj = close';

    for r in execute 'select * from ' || 
        quote_ident(schema_name) || '.split order by ex_date desc'
    loop
        factor := 1.0 / (r.to_factor / r.from_factor);        
        execute 'update ' || quote_ident(schema_name) || '.price set ' || 
            ' factor_adj = factor_adj * ' || factor ||  
            ' where date < ''' || r.ex_date || '''';
    end loop;

    for r in execute 'select d.ex_date, d.amount, p.close from ' || 
        quote_ident(schema_name) || '.dividend d left join ' || 
        quote_ident(schema_name) || '.price p on d.ex_date = p.date where ' || 
        ' d.payment_type in (''Cash'', ''Cash&Stock'') ' || 
        ' order by d.ex_date desc'
    loop
        factor :=  r.close / (r.close + r.amount);
        execute 'update ' || quote_ident(schema_name) || '.price set ' || 
            ' factor_adj = factor_adj * ' || factor || 
            ' where date < ''' || r.ex_date || '''';
    end loop;

    execute 'update ' || quote_ident(schema_name) || '.price set ' || 
        ' factor_adj = round(factor_adj, 4) ' 
        ', close_adj = round(close * factor_adj, 4) ';

    -- todo adjust by dividends (stock, cash&stock)

end $$;

