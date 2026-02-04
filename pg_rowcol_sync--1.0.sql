create publication htap_pub;

CREATE TABLE IF NOT EXISTS ddl_queue (
    id         BIGSERIAL PRIMARY KEY,
    ddl_sql    TEXT        NOT NULL,
    ddl_type   TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

alter publication htap_pub add table ddl_queue;

CREATE OR REPLACE FUNCTION capture_ddl_to_queue()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    cmd      RECORD;
    ddl_sql  TEXT;
    new_sql  TEXT;
    tbl_name TEXT;
BEGIN
    ddl_sql := current_query();

    -- If query text is unavailable, do nothing
    IF ddl_sql IS NULL THEN
        RETURN;
    END IF;

    FOR cmd IN
        SELECT *
        FROM pg_event_trigger_ddl_commands()
        WHERE object_type = 'table'
          AND (command_tag LIKE 'CREATE TABLE%'
               OR command_tag LIKE 'ALTER TABLE%')
    LOOP
        -- Extract bare table name (strip schema if present)
        tbl_name := split_part(cmd.object_identity, '.', 2);
        IF tbl_name = '' THEN
            tbl_name := cmd.object_identity;
        END IF;

        ----------------------------------------------------------------
        -- 🔴 SKIP if this is already a columnar (_col) table
        ----------------------------------------------------------------
        IF tbl_name ~ '_col$' THEN
            CONTINUE;
        END IF;

        /*
         * Rewrite:
         *   CREATE TABLE <table>
         *   ALTER TABLE <table>
         * → target <table>_col
         */
        new_sql := regexp_replace(
            ddl_sql,
            '^(CREATE|ALTER)\s+TABLE\s+((IF\s+NOT\s+EXISTS\s+)?)((\w+\.)?)'
            || tbl_name,
            '\1 TABLE \4' || tbl_name || '_col',
            'i'
        );

        -- CREATE TABLE → append USING columnar
        IF cmd.command_tag LIKE 'CREATE TABLE%' THEN
            new_sql := regexp_replace(
                new_sql,
                ';\s*$',
                ' USING columnar;',
                'g'
            );
        END IF;

        INSERT INTO ddl_queue (ddl_sql, ddl_type)
        VALUES (new_sql, cmd.command_tag);
    END LOOP;
END;
$$;



DROP EVENT TRIGGER IF EXISTS ddl_queue_trigger;

CREATE EVENT TRIGGER ddl_queue_trigger
ON ddl_command_end
EXECUTE FUNCTION capture_ddl_to_queue();

-- trigger to publish rowstores

CREATE OR REPLACE FUNCTION add_table_to_publication()
RETURNS event_trigger
LANGUAGE plpgsql
AS $$
DECLARE
    cmd RECORD;
    rel REGCLASS;
    relname TEXT;
BEGIN
    FOR cmd IN
        SELECT *
        FROM pg_event_trigger_ddl_commands()
        WHERE command_tag = 'CREATE TABLE'
          AND object_type = 'table'
          AND schema_name NOT IN ('pg_catalog', 'information_schema')
    LOOP
        rel := cmd.object_identity::regclass;
        relname := rel::text;

        -- 🔴 Skip columnstore tables (_col suffix)
        IF relname ~ '_col$' THEN
            RAISE LOG 'Skipping columnar table %', relname;
            CONTINUE;
        END IF;

        EXECUTE format(
            'ALTER PUBLICATION htap_pub ADD TABLE %s',
            rel
        );
    END LOOP;
END;
$$;


DROP EVENT TRIGGER IF EXISTS add_table_to_publication_trigger;

CREATE EVENT TRIGGER add_table_to_publication_trigger
ON ddl_command_end
EXECUTE FUNCTION add_table_to_publication();
