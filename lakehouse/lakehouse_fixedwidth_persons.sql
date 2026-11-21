-- each prod216 file is a complete snapshot, so replace the table contents rather than appending.
-- expects variable 'file' to be set to the latest unloaded persons_YYYY-MM-DD.parquet file.

BEGIN TRANSACTION;

DELETE FROM fixedwidth.prod216_persons;

INSERT INTO fixedwidth.prod216_persons BY NAME (
    SELECT regexp_extract(getvariable('file'), '(\d{4}-\d{2}-\d{2})\.parquet$', 1)::DATE AS production_date,
           trim("Company Number")                                                    AS company_number,
           nullif(trim("App Date Origin"), '')                                       AS app_date_origin,
           nullif(trim("Appointment Type"), '')                                      AS appointment_type,
           nullif(trim("Person number"), '')                                         AS person_number,
           trim("Corporate indicator") = 'Y'                                         AS corporate_indicator,
           try_strptime(trim("Appointment Date"), '%Y%m%d')::DATE                    AS appointment_date,
           try_strptime(trim("Resignation Date"), '%Y%m%d')::DATE                    AS resignation_date,
           nullif(trim("Person Postcode"), '')                                       AS person_postcode,
           -- partial date of birth is YYYYMM, stored as the first of the month
           try_strptime(trim("Partial Date of Birth"), '%Y%m')::DATE                 AS partial_date_of_birth,
           try_strptime(trim("Full Date of Birth"), '%Y%m%d')::DATE                  AS full_date_of_birth,
           nullif(trim(Title), '')                                                   AS title,
           nullif(trim(Forenames), '')                                               AS forenames,
           nullif(trim(Surname), '')                                                 AS surname,
           nullif(trim(Honours), '')                                                 AS honours,
           nullif(trim(Care_of), '')                                                 AS care_of,
           nullif(trim(PO_box), '')                                                  AS po_box,
           nullif(trim("Address line 1"), '')                                        AS address_line_1,
           nullif(trim("Address line 2"), '')                                        AS address_line_2,
           nullif(trim(Post_town), '')                                               AS post_town,
           nullif(trim(County), '')                                                  AS county,
           nullif(trim(Country), '')                                                 AS country,
           nullif(trim(Occupation), '')                                              AS occupation,
           nullif(trim(Nationality), '')                                             AS nationality,
           nullif(trim("Resident Country"), '')                                      AS resident_country
    FROM read_parquet(getvariable('file'))
);

COMMIT;

-- mark this file and any older superseded snapshots as loaded
INSERT INTO catalogue.cc_metadata.loaded_files
SELECT file FROM glob(getvariable('searchPath'))
WHERE file <= getvariable('file')
  AND file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files);
