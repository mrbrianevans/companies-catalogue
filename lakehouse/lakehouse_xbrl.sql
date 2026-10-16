-- ch-xbrl

SET VARIABLE files = (
    SELECT list(file)
    FROM (
        FROM glob('s3://' || getvariable('SINK_BUCKET') || '/ch-xbrl/*.parquet')
        WHERE file NOT IN (SELECT file FROM catalogue.cc_metadata.loaded_files)
        ORDER BY file ASC
        LIMIT 1
    )
);

SELECT getvariable('files');

CREATE OR REPLACE TEMPORARY TABLE facts AS
       SELECT source_file, company_number, concept, value, dimensions, period_end, unit,
           date(filename[-30:-21]) as zip_start, cast(filename[-18:-9] as date) as zip_end, filename as archive
       FROM read_parquet(getvariable('files'));

-- this lakehouse step will be the PIVOT sql to convert the raw fact parquet files to one line per filing with columns for common concepts


CREATE OR REPLACE TEMPORARY TABLE alias(col, ns_like, local_name, is_money) AS VALUES
--   ('employees',          'http://xbrl.frc.org.uk/fr/%/core',            'AverageNumberEmployeesDuringPeriod', false),
  ('employees',          'http://www.xbrl.org/uk/%/core/2009-09-01',    'AverageNumberEmployeesDuringPeriod', false),
  ('employees',          'http://xbrl.frc.org.uk/reports/%/direp',      'EmployeesTotal',                     false),
  ('cash',               'http://xbrl.frc.org.uk/fr/%/core',            'CashBankOnHand',                     true),
  ('cash',               'http://www.xbrl.org/uk/gaap/core/2009-09-01', 'CashBankInHand',                     true),
  ('net_assets',         'http://xbrl.frc.org.uk/fr/%/core',            'NetAssetsLiabilities',               true),
--   ('net_assets',         'http://www.xbrl.org/uk/ifrs/core/2009-09-01', 'NetAssetsLiabilities',               true),
  ('net_assets',         'http://www.xbrl.org/uk/gaap/core/2009-09-01', 'NetAssetsLiabilitiesIncludingPensionAssetLiability', true),
  ('equity',             'http://xbrl.frc.org.uk/fr/%/core',            'Equity',                             true),
  ('equity',             'http://www.xbrl.org/uk/gaap/core/2009-09-01', 'ShareholderFunds',                   true),
--   ('net_current_assets', 'http://xbrl.frc.org.uk/fr/%/core',            'NetCurrentAssetsLiabilities',        true),
  ('net_current_assets', 'http://www.xbrl.org/uk/%/core/2009-09-01',    'NetCurrentAssetsLiabilities',        true);

-- Report period per file, from dimensionless facts.
CREATE OR REPLACE TEMPORARY TABLE report AS
SELECT archive, source_file, any_value(company_number) AS company_number,
       coalesce(max(value) FILTER (WHERE concept = 'EndDateForPeriodCoveredByReport'),
                max(value) FILTER (WHERE concept = 'BalanceSheetDate'))::DATE AS period_end,
    max(value) FILTER (WHERE concept = 'StartDateForPeriodCoveredByReport')::DATE AS period_start
FROM facts WHERE dimensions IS NULL GROUP BY archive, source_file;

-- Dimensionless, current-period numeric facts for the chosen columns.
CREATE OR REPLACE TEMPORARY TABLE cur AS
SELECT f.archive, f.source_file, a.col, a.is_money, f.unit, try_cast(f.value AS DOUBLE) AS v
FROM facts f JOIN report r USING (archive, source_file)
             JOIN alias a ON f.concept = a.local_name
WHERE f.dimensions IS NULL AND f.period_end::DATE = r.period_end
  AND try_cast(f.value AS DOUBLE) IS NOT NULL;

-- Reporting currency: most common currency among the money facts.
CREATE OR REPLACE TEMPORARY TABLE currency AS
SELECT archive, source_file, replace(mode(unit), 'iso4217:', '') AS currency
FROM cur WHERE is_money AND unit LIKE 'iso4217:%' GROUP BY archive, source_file;

-- Text columns: any dimensions allowed, dimensionless fact preferred.
CREATE OR REPLACE TEMPORARY TABLE text_cols AS
SELECT archive, source_file,
       arg_min(value, coalesce(dimensions, '')) FILTER (WHERE concept = 'NameProductionSoftware') AS software,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE concept = 'NameEntityAccountants')  AS accountant_name,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE concept = 'NameEntityAuditors')     AS auditor_name,
    min(json_extract_string(dimensions, '$.AccountsStatusDimension'))
        FILTER (WHERE concept = 'AccountsStatusAuditedOrUnaudited')                           AS accounts_status,
    min(json_extract_string(dimensions, '$.AccountsTypeDimension'))
        FILTER (WHERE concept = 'AccountsType')                                               AS accounts_type,
    min(json_extract_string(dimensions, '$.AccountingStandardsDimension'))
        FILTER (WHERE concept = 'AccountingStandardsApplied')                                 AS accounting_standard,
    bool_or(lower(trim(value)) IN ('true', '1'))
        FILTER (WHERE concept = 'EntityDormantTruefalse')                                     AS dormant,
    CASE WHEN bool_or(concept = 'EntityTradingStatus') THEN coalesce(
            min(json_extract_string(dimensions, '$.EntityTradingStatusDimension')) FILTER (WHERE concept = 'EntityTradingStatus'),
            'Trading') END                                                                        AS trading_status,
       arg_min(value, coalesce(dimensions, '')) FILTER (WHERE concept = 'EntityCurrentLegalOrRegisteredName') AS company_name,
    min(json_extract_string(dimensions, '$.LegalFormEntityDimension'))
        FILTER (WHERE concept = 'LegalFormEntity')                                            AS legal_form
FROM facts
WHERE concept IN ('NameProductionSoftware', 'NameEntityAccountants', 'NameEntityAuditors', 'AccountsStatusAuditedOrUnaudited',
                  'AccountsType', 'AccountingStandardsApplied', 'EntityDormantTruefalse', 'EntityTradingStatus',
                  'EntityCurrentLegalOrRegisteredName', 'LegalFormEntity')
GROUP BY archive, source_file;

CREATE OR REPLACE TEMPORARY TABLE accounts_pivot AS
SELECT r.archive, r.source_file, r.company_number, t.company_name, r.period_start, r.period_end, c.currency,
       p.employees, p.cash, p.net_assets, p.equity, p.net_current_assets,
       t.software, t.accountant_name, t.auditor_name, t.accounts_status,
       t.accounts_type, t.accounting_standard, t.dormant, t.trading_status, t.legal_form
FROM report r
         LEFT JOIN (
    PIVOT (SELECT archive, source_file, col, v FROM cur)
        ON col IN ('employees', 'cash', 'net_assets', 'equity', 'net_current_assets')
  USING max(v)
  GROUP BY archive, source_file
    ) p USING (archive, source_file)
         LEFT JOIN currency c USING (archive, source_file)
         LEFT JOIN text_cols t USING (archive, source_file);


-- insert new pivot rows into lakehouse
INSERT INTO accounts BY NAME (
    SELECT *
    FROM accounts_pivot
);

INSERT INTO catalogue.cc_metadata.loaded_files
FROM UNNEST(getvariable('files'));
