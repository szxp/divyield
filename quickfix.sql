DO $$
DECLARE 
    r record;
    cur_count integer; 
BEGIN


--        EXECUTE 'alter table public.profile ' || 
--            'add column pulled timestamp with time zone';


    FOR r IN select schema_name 
        from information_schema.schemata 
        where schema_name like 's_%' 
        order by schema_name asc
    LOOP

--        EXECUTE 'drop table ' || 
--            quote_ident(r.schema_name) || '.profile cascade';

--        EXECUTE 'call public.init_schema_tables(''' || 
--            quote_ident(r.schema_name) || ''')';
        
--        EXECUTE 'call public.init_schema_views(''' || 
--            quote_ident(r.schema_name) || ''')';

--         EXECUTE 'alter table ' || 
--             quote_ident(r.schema_name) || 
--             '.price add column created timestamp with time zone';
--         EXECUTE 'update ' || 
--             quote_ident(r.schema_name) || 
--             '.dividend set created = current_timestamp';

--         EXECUTE 'alter table ' || 
--             quote_ident(r.schema_name) || 
--             '.split add column created timestamp with time zone';
--
--        EXECUTE 'update ' || 
--            quote_ident(r.schema_name) || 
--            '.price set currency = ''USD'' ';
--
--        EXECUTE 'alter table ' || 
--            quote_ident(r.schema_name) || 
--            '.price alter column currency set not null ';
        
        --EXECUTE 'select count(distinct currency) from ' || quote_ident(r.schema_name) || '.dividend' into cur_count;
        --if cur_count > 1 then
        --    raise notice '%', r.schema_name;
        --end if;


    END LOOP;
END $$;



