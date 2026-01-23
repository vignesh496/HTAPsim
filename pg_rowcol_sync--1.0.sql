
-- 1. FUNCTION: Create columnstore mirror + add rowstore to publication

CREATE OR REPLACE FUNCTION trg_create_columnar_mirror()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
    row_table_name text;
    col_table_name text;
    schema_name text;
    v_pub_name text := 'pub_sales';
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.command_tag = 'CREATE TABLE'
           AND obj.schema_name NOT LIKE 'pg_%' THEN

            schema_name := obj.schema_name;
            row_table_name :=
                (parse_ident(obj.object_identity))
                [array_upper(parse_ident(obj.object_identity), 1)];

            -- Skip mirror tables
            IF row_table_name LIKE '%\_col' ESCAPE '\' THEN
                CONTINUE;
            END IF;

            col_table_name := row_table_name || '_col';

            -- Create columnar mirror
            EXECUTE format(
                'CREATE TABLE %I.%I (LIKE %I.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS) USING columnar',
                schema_name, col_table_name,
                schema_name, row_table_name
            );

            -- Add rowstore to publication
            EXECUTE format(
                'ALTER PUBLICATION %I ADD TABLE %I.%I',
                v_pub_name, schema_name, row_table_name
            );

            RAISE NOTICE 'Mirror created: %.% and added %.% to publication %',
                schema_name, col_table_name,
                schema_name, row_table_name,
                v_pub_name;
        END IF;
    END LOOP;
END;
$$;


-- 2. FUNCTION: Drop columnstore mirror + remove rowstore from publication

CREATE OR REPLACE FUNCTION trg_drop_columnar_mirror()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
    col_table_name text;
    v_pub_name text := 'pub_sales';
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table'
           AND obj.object_name NOT LIKE '%\_col' ESCAPE '\' THEN

            col_table_name := obj.object_name || '_col';

            -- Drop mirror
            EXECUTE format(
                'DROP TABLE IF EXISTS %I.%I CASCADE',
                obj.schema_name, col_table_name
            );

            -- Remove from publication (safe)
            BEGIN
                EXECUTE format(
                    'ALTER PUBLICATION %I DROP TABLE IF EXISTS %I.%I',
                    v_pub_name, obj.schema_name, obj.object_name
                );
            EXCEPTION WHEN OTHERS THEN
                NULL;
            END;

            RAISE NOTICE 'Mirror dropped: %.%',
                obj.schema_name, col_table_name;
        END IF;
    END LOOP;
END;
$$;


-- 3. FUNCTION: ALTER handling (RENAME TABLE + ADD/DROP COLUMN)

CREATE OR REPLACE FUNCTION trg_alter_columnar_mirror()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    obj record;
    schema_name text;
    new_row_name text;
    old_row_name text;
    new_col_name text;
    old_col_name text;
    v_sql text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.command_tag != 'ALTER TABLE'
           OR obj.schema_name LIKE 'pg_%' THEN
            CONTINUE;
        END IF;

        schema_name := obj.schema_name;
        new_row_name :=
            (parse_ident(obj.object_identity))
            [array_upper(parse_ident(obj.object_identity), 1)];

        -- Skip mirror table alterations
        IF new_row_name LIKE '%\_col' ESCAPE '\' THEN
            CONTINUE;
        END IF;

        new_col_name := new_row_name || '_col';

        
        -- RENAME TABLE HANDLING (ONLY if SQL says RENAME)

        IF current_query() ~* 'ALTER\s+TABLE\s+.*\s+RENAME\s+TO\s+' THEN

            old_row_name :=
                regexp_replace(
                    current_query(),
                    '.*ALTER\s+TABLE\s+([^\s]+)\s+RENAME\s+TO\s+([^\s]+).*',
                    '\1',
                    'i'
                );

            old_row_name :=
                (parse_ident(old_row_name))
                [array_upper(parse_ident(old_row_name), 1)];

            old_col_name := old_row_name || '_col';

            EXECUTE format(
                'ALTER TABLE %I.%I RENAME TO %I',
                schema_name, old_col_name, new_col_name
            );

            RAISE NOTICE 'Mirror rename: %.% → %.%',
                schema_name, old_col_name,
                schema_name, new_col_name;

            RETURN;
        END IF;

        -- COLUMN ADD / DROP HANDLING
        
        v_sql := format(
            'ALTER TABLE %I.%I %s',
            schema_name,
            new_col_name,
            substring(
                current_query()
                from '(?i)ALTER\s+TABLE\s+[^\s]+\s+(.*)'
            )
        );

        BEGIN
            EXECUTE v_sql;
            RAISE NOTICE 'Mirror altered: %.%', schema_name, new_col_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING
                'Mirror alter skipped for %.% : %',
                schema_name, new_col_name, SQLERRM;
        END;

    END LOOP;
END;
$$;

-- EVENT TRIGGERS

DROP EVENT TRIGGER IF EXISTS evt_columnar_mirror_on_create;
CREATE EVENT TRIGGER evt_columnar_mirror_on_create
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE')
EXECUTE FUNCTION trg_create_columnar_mirror();

DROP EVENT TRIGGER IF EXISTS evt_drop_mirror;
CREATE EVENT TRIGGER evt_drop_mirror
ON sql_drop
EXECUTE FUNCTION trg_drop_columnar_mirror();

DROP EVENT TRIGGER IF EXISTS evt_alter_mirror;
CREATE EVENT TRIGGER evt_alter_mirror
ON ddl_command_end
WHEN TAG IN ('ALTER TABLE')
EXECUTE FUNCTION trg_alter_columnar_mirror();
-- add our sql queries here
