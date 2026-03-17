-- create publication

DROP PUBLICATION IF EXISTS htap_pub;
create publication htap_pub;


-- DDL Queue table to track changes for the worker to process

CREATE TABLE IF NOT EXISTS ddl_queue (
    id         BIGSERIAL PRIMARY KEY,
    ddl_sql    TEXT        NOT NULL,
    ddl_type   TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

alter publication htap_pub add table ddl_queue;



-- alter table add column name, with col. type

CREATE OR REPLACE PROCEDURE htap_add(
    tbl_name TEXT,
    col_name TEXT,
    col_type TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = tbl_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Skipping ADD COLUMN.', tbl_name;
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = tbl_name
          AND column_name = col_name
    ) THEN
        RAISE NOTICE 'Column "%" already exists on table "%".', col_name, tbl_name;
        RETURN;
    END IF;

    EXECUTE format(
        'ALTER TABLE %I ADD COLUMN %I %s',
        tbl_name, col_name, col_type
    );

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('ALTER TABLE %I_col ADD COLUMN %I %s',
               tbl_name, col_name, col_type),
        'ALTER'
    );

    RAISE LOG 'Added column "%" to table "%".', col_name, tbl_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_add failed: %', SQLERRM;
        RAISE;
END;
$$;

-- alter table drop column 

CREATE OR REPLACE PROCEDURE htap_drop_col(
    tbl_name TEXT,
    col_name TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = tbl_name
          AND column_name = col_name
    ) THEN
        RAISE NOTICE 'Column "%" does not exist on table "%". Skipping.', col_name, tbl_name;
        RETURN;
    END IF;

    EXECUTE format('ALTER TABLE %I DROP COLUMN %I', tbl_name, col_name);

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('ALTER TABLE %I_col DROP COLUMN %I', tbl_name, col_name),
        'ALTER'
    );

    RAISE LOG 'Dropped column "%" from table "%".', col_name, tbl_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_drop_col failed: %', SQLERRM;
        RAISE;
END;
$$;

-- alter table rename from_table to to_table name

CREATE OR REPLACE PROCEDURE htap_rename_table(
    old_name TEXT,
    new_name TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = old_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Skipping rename.', old_name;
        RETURN;
    END IF;

    EXECUTE format('ALTER PUBLICATION htap_pub DROP TABLE %I', old_name);
    EXECUTE format('ALTER TABLE %I RENAME TO %I', old_name, new_name);
    EXECUTE format('ALTER PUBLICATION htap_pub ADD TABLE %I', new_name);

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('ALTER TABLE %I_col RENAME TO %I_col', old_name, new_name),
        'RENAME'
    );

    RAISE LOG 'Renamed table "%" to "%".', old_name, new_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_rename_table failed: %', SQLERRM;
        RAISE;
END;
$$;

-- alter table rename column

CREATE OR REPLACE PROCEDURE htap_rename_column(
    tbl_name TEXT,
    old_col  TEXT,
    new_col  TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- table exists?
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = tbl_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Skipping RENAME COLUMN.', tbl_name;
        RETURN;
    END IF;

    -- old column exists?
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = tbl_name
          AND column_name = old_col
    ) THEN
        RAISE NOTICE 'Column "%" does not exist on table "%". Skipping rename.',
                     old_col, tbl_name;
        RETURN;
    END IF;

    -- new column already exists?
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = tbl_name
          AND column_name = new_col
    ) THEN
        RAISE NOTICE 'Column "%" already exists on table "%". Skipping rename.',
                     new_col, tbl_name;
        RETURN;
    END IF;

    EXECUTE format(
        'ALTER TABLE %I RENAME COLUMN %I TO %I',
        tbl_name, old_col, new_col
    );

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('ALTER TABLE %I_col RENAME COLUMN %I TO %I',
               tbl_name, old_col, new_col),
        'RENAME'
    );

    RAISE LOG 'Renamed column "%" to "%" on table "%".',
              old_col, new_col, tbl_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_rename_column failed: %', SQLERRM;
        RAISE;
END;
$$;

-- alter table column datatype change

CREATE OR REPLACE PROCEDURE htap_change_type(
    tbl_name TEXT,
    col_name TEXT,
    new_type TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    current_type TEXT;
BEGIN
    -- table exists?
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = tbl_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Skipping ALTER TYPE.', tbl_name;
        RETURN;
    END IF;

    -- column exists?
    SELECT data_type
    INTO current_type
    FROM information_schema.columns
    WHERE table_name = tbl_name
      AND column_name = col_name;

    IF NOT FOUND THEN
        RAISE NOTICE 'Column "%" does not exist on table "%". Skipping ALTER TYPE.',
                     col_name, tbl_name;
        RETURN;
    END IF;

    -- same type → no-op
    IF lower(current_type) = lower(new_type) THEN
        RAISE NOTICE 'Column "%" on table "%" already has type "%". Skipping.',
                     col_name, tbl_name, new_type;
        RETURN;
    END IF;

    EXECUTE format(
        'ALTER TABLE %I ALTER COLUMN %I TYPE %s',
        tbl_name, col_name, new_type
    );

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('ALTER TABLE %I_col ALTER COLUMN %I TYPE %s',
               tbl_name, col_name, new_type),
        'ALTER'
    );

    RAISE LOG 'Changed column "%" type on table "%" to "%".',
              col_name, tbl_name, new_type;

EXCEPTION
    WHEN datatype_mismatch OR invalid_text_representation THEN
        RAISE NOTICE
            'Type conversion failed for "%.%" → %',
            tbl_name, col_name, new_type;
        RAISE LOG 'htap_change_type cast error: %', SQLERRM;

    WHEN OTHERS THEN
        RAISE LOG 'htap_change_type failed: %', SQLERRM;
        RAISE;
END;
$$;

-- truncate table 

CREATE OR REPLACE PROCEDURE htap_truncate(tbl_name TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = tbl_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Skipping truncate.', tbl_name;
        RETURN;
    END IF;

    EXECUTE format('TRUNCATE TABLE %I CASCADE', tbl_name);

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('TRUNCATE TABLE %I_col CASCADE', tbl_name),
        'TRUNCATE'
    );

    RAISE LOG 'Truncated table "%".', tbl_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_truncate failed: %', SQLERRM;
        RAISE;
END;
$$;

-- drop table

CREATE OR REPLACE PROCEDURE htap_drop_table(tbl_name TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = tbl_name
    ) THEN
        RAISE NOTICE 'Table "%" does not exist. Nothing to drop.', tbl_name;
        RETURN;
    END IF;

    EXECUTE format('DROP TABLE %I CASCADE', tbl_name);

    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('DROP TABLE IF EXISTS %I_col CASCADE', tbl_name),
        'DROP'
    );

    RAISE LOG 'Dropped table "%".', tbl_name;

EXCEPTION
    WHEN OTHERS THEN
        RAISE LOG 'htap_drop_table failed: %', SQLERRM;
        RAISE;
END;
$$;

-- create table with add publication

CREATE OR REPLACE PROCEDURE htap_create(tbl_name TEXT, columns_definition TEXT)
LANGUAGE plpgsql AS $$
BEGIN
    -- 1. Create the Rowstore Table (Standard Heap)
    EXECUTE format('CREATE TABLE %I (%s)', tbl_name, columns_definition);
    EXECUTE format('Alter publication htap_pub add table %I', tbl_name);
    -- 2. Create the Columnar Mirror
    -- Note: We append 'USING columnar' (or your specific engine syntax)
    EXECUTE format(
        'CREATE TABLE %I_col (%s) USING columnar', 
        tbl_name, 
        columns_definition
    );

    -- 3. Log to the DDL Queue
    -- We store the 'columnar' version so the BGWorker knows exactly what to run
    INSERT INTO ddl_queue (ddl_sql, ddl_type)
    VALUES (
        format('CREATE TABLE IF NOT EXISTS %I_col (%s) USING columnar;', tbl_name, columns_definition),
        'CREATE'
    );

    RAISE NOTICE 'Table % and its columnar mirror %_col created.', tbl_name, tbl_name;
END;
$$;