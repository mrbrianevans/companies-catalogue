CREATE TABLE IF NOT EXISTS events
(
    resource_kind
        VARCHAR,
    resource_id
        VARCHAR,
    resource_uri
        VARCHAR,
    "data"
        JSON,
    "event"
        STRUCT
            (
            timepoint
            BIGINT,
    published_at
            VARCHAR,
    "type"
            VARCHAR
) );
CREATE TABLE IF NOT EXISTS snapshot AS FROM events WITH NO DATA;

CREATE SCHEMA IF NOT EXISTS cc_metadata;

CREATE TABLE IF NOT EXISTS cc_metadata.loaded_files
(
    file
        VARCHAR
);
