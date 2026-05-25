SET VARIABLE files = (
    SELECT list(file)
    FROM (
        FROM glob('s3://' || getvariable('SINK_BUCKET') || '/xbrl/*.csv')
        WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)
        ORDER BY file ASC
        LIMIT 1
    )
);

SELECT getvariable('files');

-- Only works if there is at least one file to load. Can't load null list.
INSERT INTO xbrl BY NAME (
    SELECT *, date(filename[-26:-17]) as zip_start, cast(filename[-14:-5] as date) as zip_end, filename as csv_name
    FROM read_csv(
        getvariable('files')
    )
    WHERE error IS NULL
);

INSERT INTO catalogue.cc_metadata.loaded_files
FROM UNNEST(getvariable('files'));
