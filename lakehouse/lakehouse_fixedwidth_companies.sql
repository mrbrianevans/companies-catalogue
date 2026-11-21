-- each prod216 file is a complete snapshot, so replace the table contents rather than appending.
-- expects variable 'file' to be set to the latest unloaded companies_YYYY-MM-DD.parquet file.

BEGIN TRANSACTION;

DELETE FROM fixedwidth.prod216_companies;

INSERT INTO fixedwidth.prod216_companies BY NAME (
    SELECT regexp_extract(getvariable('file'), '(\d{4}-\d{2}-\d{2})\.parquet$', 1)::DATE AS production_date,
           trim("Company Number")                                                    AS company_number,
           nullif(trim("Company Status"), '')                                        AS company_status,
           try_cast(trim("Number of Officers") AS INTEGER)                           AS number_of_officers,
           nullif(trim("Company Name"), '')                                          AS company_name
    FROM read_parquet(getvariable('file'))
);

COMMIT;

-- mark this file and any older superseded snapshots as loaded
INSERT INTO catalogue.cc_metadata.loaded_files
SELECT file FROM glob(getvariable('searchPath'))
WHERE file <= getvariable('file')
  AND file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files);
