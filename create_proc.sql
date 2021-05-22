
create or replace procedure 
    public.init_schema_tables(schema_name text)
language plpgsql
as $$
declare 
begin
    execute 'create schema if not exists ' || 
        quote_ident(schema_name);

    execute 'create table if not exists ' || 
        quote_ident(schema_name) || '.profile (
		symbol           varchar(10) not null,
		name             text,
		exchange         text,
		issue_type       text,
		industry         text,
		sector           text,
		description      text,
		website          text,
		primary_sic_code text,
		address          text,
		state            text,
		city             text,
		zip              text,
		country          text,
		phone            text,
        PRIMARY KEY(symbol)	
    )';


    execute 'create table if not exists ' || 
        quote_ident(schema_name) || '.price (
        date        date not null,
        symbol      varchar(10) not null,
        currency    char(3) not null,
        close       numeric not null,
        high        numeric not null,
        low         numeric not null,
        open        numeric not null,
        volume      numeric not null,
        factor_adj  numeric not null default 1,
        close_adj   numeric not null default 0,
        PRIMARY KEY(date)	
    )';

    execute 'create table if not exists ' || 
        quote_ident(schema_name) || '.dividend (
        id           bigint not null,
        ex_date      date not null,
        symbol       varchar(10) not null,
        amount       numeric not null,
        currency     char(3) not null,
        frequency    smallint not null,
        payment_type text not null, 
        factor_adj   numeric not null default 1,
        amount_adj   numeric not null default 0,
        PRIMARY KEY(id)	
    )';

    execute 'create table if not exists ' || 
        quote_ident(schema_name) || '.split (
        ex_date      date not null,
        to_factor     numeric not null,
        from_factor   numeric not null,
        PRIMARY KEY(ex_date)	
    )';


end $$;

create or replace procedure public.init_schema_views(schema_name text)
language plpgsql
as $$
declare 
begin
    execute 'create or replace view ' || quote_ident(schema_name) || '.dividend_view as
        select 
            ex_date,
            symbol, 
            sum(amount) amount, 
            currency, 
            frequency, 
            payment_type, 
            factor_adj, 
            sum(amount_adj) amount_adj
        from ' || quote_ident(schema_name) || '.dividend
        group by 
            ex_date, 
            symbol, 
            currency, 
            frequency, 
            payment_type, 
            factor_adj
        order by ex_date desc';
end $$;






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

    for r in execute 'select d.ex_date, d.amount, coalesce(p.close, 0) as close from ' || 
        quote_ident(schema_name) || '.dividend d left join ' || 
        quote_ident(schema_name) || '.price p on d.ex_date = p.date where ' || 
        ' d.payment_type in (''Cash'', ''Cash&Stock'') ' || 
        ' order by d.ex_date desc'
    loop
        if r.close > 0 then
            factor :=  r.close / (r.close + r.amount);
            
            execute 'update ' || quote_ident(schema_name) || '.price set ' || 
                ' factor_adj = factor_adj * ' || factor || 
                ' where date < ''' || r.ex_date || '''';
        end if;
    end loop;

    execute 'update ' || quote_ident(schema_name) || '.price set ' || 
        ' factor_adj = round(factor_adj, 4) ' 
        ', close_adj = round(close * factor_adj, 4) ';

end $$;

