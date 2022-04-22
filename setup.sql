CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE etl.dependency (
    schema_name TEXT NOT NULL
,   table_name TEXT NOT NULL
,   dependency_schema_name TEXT NOT NULL
,   dependency_table_name TEXT NOT NULL
,   dependency_field_name TEXT NOT NULL
,   seen TIMESTAMPTZ(0) NULL
,   PRIMARY KEY (
        table_name
    ,   schema_name
    ,   dependency_table_name
    ,   dependency_field_name
    ,   dependency_schema_name
    )
);

INSERT INTO etl.dependency(
    schema_name
,   table_name
,   dependency_schema_name
,   dependency_table_name
,   dependency_field_name
,   seen
)
VALUES
(
    'dv' -- schema_name
,   's_customer_sales' -- table_name
,   'sales' --- dependency_schema_name
,   'customer' -- dependency_table_name
,   'date_added' -- dependency_field_name
,   '2010-01-01' -- seen
),
(
    'dv' -- schema_name
,   's_customer_sales' -- table_name
,   'sales' --- dependency_schema_name
,   'customer' -- dependency_table_name
,   'date_updated' -- dependency_field_name
,   NULL -- seen
),
(
    'dv' -- schema_name
,   's_customer_sales' -- table_name
,   'dv' --- dependency_schema_name
,   'h_customer' -- dependency_table_name
,   'ts' -- dependency_field_name
,   NULL -- seen
);

CREATE TABLE IF NOT EXISTS etl.load (
    schema_name     TEXT NOT NULL
,   table_name      TEXT NOT NULL
,   field_name      TEXT NOT NULL
,   ts              TIMESTAMPTZ(0) NOT NULL
,   PRIMARY KEY (table_name, schema_name, field_name)
);

INSERT INTO etl.load (
    schema_name
,   table_name
,   field_name
,   ts
)
VALUES
    ('sales', 'customer', 'date_added', '2010-01-01 04:00:00')
,   ('sales', 'customer', 'date_updated', '2010-02-02 02:00:00')
;

CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.customer (
    customer_id SERIAL PRIMARY KEY
,   first_name TEXT NOT NULL
,   date_added TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
,   date_updated TIMESTAMPTZ(0) NOT NULL
);

CREATE SCHEMA IF NOT EXISTS dv;
CREATE TABLE IF NOT EXISTS dv.s_customer_sales (
    h_customer_hk CHAR(32) NOT NULL
,   first_name TEXT NOT NULL
,   last_name TEXT NOT NULL
,   hd CHAR(32) NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE PROCEDURE dv.refresh_s_customer_sales (
    p_incremental BOOLEAN DEFAULT TRUE
,   p_dv_h_customer_ts TIMESTAMPTZ(0) DEFAULT '1900-01-01 00:00:00 +0'
,   p_sales_customer_date_added TIMESTAMPTZ(0) DEFAULT '1900-01-01 00:00:00 +0'
,   p_sales_customer_date_updated TIMESTAMPTZ(0) DEFAULT '1900-01-01 00:00:00 +0'
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'p_incremental: %', p_incremental;
    RAISE NOTICE 'p_dv_h_customer_ts: %', p_dv_h_customer_ts;
    RAISE NOTICE 'p_sales_customer_date_added: %', p_sales_customer_date_added;
    RAISE NOTICE 'p_sales_customer_date_updated: %', p_sales_customer_date_updated;
END;
$$;
CALL dv.refresh_s_customer_sales();

CALL dv.refresh_s_customer_sales();