DO $$
DECLARE 
    r record;
    cur_count integer; 
BEGIN
    FOR r IN select schema_name from information_schema.schemata where schema_name like 's_%' order by schema_name asc
    LOOP
        EXECUTE 'select count(distinct currency) from ' || quote_ident(r.schema_name) || '.dividend' into cur_count;
        if cur_count > 1 then
            raise notice '%', r.schema_name;
        end if;
    END LOOP;
END $$;



