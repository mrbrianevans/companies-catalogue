-- ch-xbrl

SET VARIABLE files = (
    SELECT list(file)
    FROM (
        FROM glob('s3://' || getvariable('SINK_BUCKET') || '/ch-xbrl/*.csv.zst')
        WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)
        ORDER BY file ASC
        LIMIT 1
    )
);

SELECT getvariable('files');

-- Only works if there is at least one file to load. Can't load null list.
INSERT INTO ch_xbrl BY NAME (
    SELECT *, date(filename[-30:-21]) as zip_start, cast(filename[-18:-9] as date) as zip_end, filename as csv_name
    FROM read_csv(
        getvariable('files'), types = {'decimals':'VARCHAR'}
    )
);

INSERT INTO catalogue.cc_metadata.loaded_files
FROM UNNEST(getvariable('files'));
