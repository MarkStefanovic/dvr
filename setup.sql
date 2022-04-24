CREATE SCHEMA IF NOT EXISTS dvr;

CREATE TABLE IF NOT EXISTS dvr.status (
    schema_name TEXT NOT NULL
,   sp_name TEXT NOT NULL
,   status TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()

,   PRIMARY KEY (sp_name, schema_name)
);

CREATE TABLE IF NOT EXISTS dvr.error (
    schema_name TEXT NOT NULL
,   sp_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL
,   error_message TEXT NOT NULL
,   PRIMARY KEY (sp_name, schema_name)
);

CREATE TABLE IF NOT EXISTS dvr.execution_time (
    schema_name TEXT NOT NULL
,   sp_name TEXT NOT NULL
,   ct INT NOT NULL
,   millis BIGINT NOT NULL
,   latest_millis BIGINT NOT NULL
,   max_millis BIGINT NOT NULL
,   min_millis BIGINT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
,   PRIMARY KEY (sp_name, schema_name)
);

CREATE TABLE dvr.dependency (
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

CREATE TABLE IF NOT EXISTS dvr.load (
    schema_name     TEXT NOT NULL
,   table_name      TEXT NOT NULL
,   field_name      TEXT NOT NULL
,   ts              TIMESTAMPTZ(0) NOT NULL
,   PRIMARY KEY (table_name, schema_name, field_name)
);

CREATE TABLE IF NOT EXISTS dvr.skip (
    schema_name TEXT NOT NULL
,   sp_name TEXT NOT NULL
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT NOW()
,   PRIMARY KEY (sp_name, schema_name)
);