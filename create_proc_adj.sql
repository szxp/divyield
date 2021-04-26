create or replace procedure public.update_dividend_adj(schema_name text)
language plpgsql
as $$
declare 
    r record;
    factor numeric; 
begin
    execute 'update ' || quote_ident(schema_name) || '.dividend set factor_adj = 1, amount_adj = amount';

    for r in execute 'select * from ' || quote_ident(schema_name) || '.split order by ex_date desc'
    loop
        factor := 1.0 / (r.to_factor / r.from_factor);
        
        execute 'update ' || quote_ident(schema_name) || '.dividend set ' || 
            ' factor_adj = factor_adj * ' || factor || 
            ', amount_adj = amount * factor_adj * ' || factor || 
            ' where ex_date <= ''' || r.ex_date || '''';
    end loop;
end $$;


create or replace procedure public.update_price_adj(schema_name text)
language plpgsql
as $$
declare 
    r record;
    factor numeric; 
begin
    execute 'update ' || quote_ident(schema_name) || '.price set factor_adj = 1, close_adj = close';

    for r in execute 'select * from ' || quote_ident(schema_name) || '.split order by ex_date desc'
    loop
        factor := 1.0 / (r.to_factor / r.from_factor);
        
        execute 'update ' || quote_ident(schema_name) || '.price set ' || 
            ' factor_adj = factor_adj * ' || factor || 
            ', close_adj = close * factor_adj * ' || factor || 
            ' where date <= ''' || r.ex_date || '''';
    end loop;

    -- todo adjust by dividends (cash, stock, cash&stock)

end $$;

