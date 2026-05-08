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

-- sorted table
ALTER TABLE events SET SORTED BY (struct_extract(event, 'timepoint') ASC);

CREATE TABLE IF NOT EXISTS catalogue.cc_metadata.loaded_files
(
    file
        VARCHAR
);
