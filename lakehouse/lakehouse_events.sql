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

SET
VARIABLE files = (SELECT list(file) FROM
(FROM glob('s3://'||getvariable('SINK_BUCKET')||'/'||getvariable('streamPath')||'/*.json.gz')
WHERE file NOT IN (SELECT file FROM cc_metadata.loaded_files)
ORDER BY file ASC
LIMIT 1));
SELECT getvariable('files');

BEGIN
TRANSACTION;

-- Only works if there is at least one file to load. Can't load null list.
INSERT INTO events BY NAME
            (FROM read_json(getvariable('files'), columns = {resource_kind : 'VARCHAR',
    resource_id : 'VARCHAR',
    resource_uri : 'VARCHAR',
    DATA : 'JSON',
    event : 'STRUCT(timepoint BIGINT, published_at VARCHAR, type VARCHAR)'}, auto_detect = FALSE)
    WHERE event.timepoint IS NOT NULL AND event.timepoint > (SELECT COALESCE(MAX(inner_events.event.timepoint), 0) FROM events inner_events)
    );

INSERT INTO cc_metadata.loaded_files
FROM UNNEST(getvariable('files'));

COMMIT;