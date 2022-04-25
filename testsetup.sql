CREATE SCHEMA IF NOT EXISTS dv;
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE sales.customer (
    customer_id SERIAL PRIMARY KEY
,   first_name TEXT NOT NULL
,   date_added TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
,   date_updated TIMESTAMPTZ(0) NOT NULL
);

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

    PERFORM pg_sleep(10);
END;
$$;
CALL dv.refresh_s_customer_sales();

INSERT INTO dvr.dependency(
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
)
ON CONFLICT (
    table_name
,   schema_name
,   dependency_table_name
,   dependency_field_name
,   dependency_schema_name
)
DO UPDATE SET
    seen = EXCLUDED.seen
;

INSERT INTO dvr.load (
    schema_name
,   table_name
,   field_name
,   ts
)
VALUES
    ('sales', 'customer', 'date_added', '2010-01-01 04:00:00')
,   ('sales', 'customer', 'date_updated', '1900-01-01 00:00:00 +0')
ON CONFLICT (table_name, schema_name, field_name)
DO UPDATE SET
    ts = NOW()
;

INSERT INTO dvr.execution_time (
    schema_name
,	sp_name
,	ct
,	millis
,	latest_millis
,	max_millis
,	min_millis
,   ts
)
VALUES (
    'dv'
,	'refresh_s_customer_sales'
,	3
,	34
,	27
,	80
,   5
,	'2020-01-02 03:04:05 +0'::TIMESTAMPTZ(0)
)
ON CONFLICT (sp_name, schema_name)
DO UPDATE SET
    ct = EXCLUDED.ct
,	millis = EXCLUDED.millis
,	latest_millis = EXCLUDED.latest_millis
,	min_millis = EXCLUDED.min_millis
,	max_millis = EXCLUDED.max_millis
,	ts = EXCLUDED.ts
;

TRUNCATE TABLE dvr.skip;