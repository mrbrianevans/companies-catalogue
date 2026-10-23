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
       SELECT source_file, source_file[14:21] as company_number, concept, value, dimensions, period_end, unit,
           filename[-30:-21] as zip_start, filename[-18:-9] as zip_end, filename as archive
       FROM read_parquet(getvariable('files'));

-- this lakehouse step will be the PIVOT sql to convert the raw fact parquet files to one line per filing with columns for common concepts


CREATE OR REPLACE TEMPORARY TABLE alias AS
SELECT * FROM read_csv('lakehouse/xbrl_aliases.csv');


-- Report period per file, from dimensionless facts.
CREATE OR REPLACE TEMPORARY TABLE report AS
SELECT archive, source_file, any_value(company_number) AS company_number,
       any_value(zip_start) AS zip_start, any_value(zip_end) AS zip_end,
       coalesce(max(value) FILTER (WHERE concept = 'EndDateForPeriodCoveredByReport'),
                max(value) FILTER (WHERE concept = 'BalanceSheetDate'))::DATE AS period_end,
    max(value) FILTER (WHERE concept = 'StartDateForPeriodCoveredByReport')::DATE AS period_start
FROM facts WHERE dimensions IS NULL GROUP BY archive, source_file;

-- Current-period numeric facts for the chosen columns, matched on exact dimensions.
CREATE OR REPLACE TEMPORARY TABLE cur AS
SELECT f.archive, f.source_file, a.col, a.priority, a.is_money, f.unit, try_cast(f.value AS DOUBLE) AS v
FROM facts f JOIN report r USING (archive, source_file)
             JOIN alias a ON f.concept = a.local_name
                         AND f.dimensions IS NOT DISTINCT FROM a.dimensions
WHERE f.period_end::DATE = r.period_end
  AND try_cast(f.value AS DOUBLE) IS NOT NULL;

-- Reporting currency: most common currency among the money facts.
CREATE OR REPLACE TEMPORARY TABLE currency AS
SELECT archive, source_file, replace(mode(unit), 'iso4217:', '') AS currency
FROM cur WHERE is_money AND unit LIKE 'iso4217:%' GROUP BY archive, source_file;

-- Text columns: any dimensions allowed, dimensionless fact preferred.
CREATE OR REPLACE TEMPORARY TABLE text_cols AS
WITH g AS (
  SELECT archive, source_file, concept AS cn, value, dimensions,
    lower(trim(value)) AS lv,
    coalesce(lower(trim(value)), 'true') NOT IN ('false', '0') AS affirmed
  FROM facts
  WHERE concept IN (
    'NameProductionSoftware', 'NameEntityAccountants', 'NameAccountants', 'NameEntityAuditors',
    'NameThirdPartyAgent', 'NameIndividualAuditorOrAccountant',
    'AccountsStatusAuditedOrUnaudited',
    'MembersHaveNotRequiredCompanyToObtainAnAudit',
    'CompanyEntitledToExemptionUnderSection477CompaniesAct2006',
    'CompanyEntitledToExemptionUnderSection480CompaniesAct2006',
    'CompanyEntitledToExemptionUnderSection480CompaniesAct2006RelatingToDormantCompanies',
    'CompanyEntitledToExemptionUnderSection249A1CompaniesAct1985',
    'CompanyEntitledToExemptionUnderSection249AA1CompaniesAct1985',
    'OpinionAuditorsOnEntity', 'NameSeniorStatutoryAuditor',
    'AccountsType',
    'AccountsAreInAccordanceWithSpecialProvisionsCompaniesActRelatingToSmallCompanies',
    'AccountsAreInAccordanceWithSpecialProvisionsInSection4453CompaniesActRelatingToMedium-sizedCompanies',
    'BusinessReportName',
    'AccountingStandardsApplied',
    'AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSSE',
    'AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSSE05',
    'AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSE',
    'FRSSEVersionDate',
    'EntityDormantTruefalse', 'EntityDormant', 'CompanyDormant', 'CompanyNotDormant',
    'EntityTradingStatus', 'EntityTrading',
    'EntityCurrentLegalOrRegisteredName', 'EntityCurrentLegalName',
    'LegalFormEntity', 'LegalFormOfEntity')
), a AS (
  SELECT archive, source_file,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'NameProductionSoftware') AS software,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'NameEntityAccountants') AS acc_new,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'NameAccountants') AS acc_old,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'NameEntityAuditors') AS aud_new,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn IN ('NameThirdPartyAgent', 'NameIndividualAuditorOrAccountant')
        AND json_extract_string(dimensions, '$.ThirdPartyAgentTypeDimension') = 'EntityAccountantsOrAuditors') AS agent_aa,
    min(json_extract_string(dimensions, '$.AccountsStatusDimension')) FILTER (WHERE cn = 'AccountsStatusAuditedOrUnaudited') AS status_new,
    bool_or(affirmed) FILTER (WHERE cn IN (
        'MembersHaveNotRequiredCompanyToObtainAnAudit',
        'CompanyEntitledToExemptionUnderSection477CompaniesAct2006',
        'CompanyEntitledToExemptionUnderSection480CompaniesAct2006',
        'CompanyEntitledToExemptionUnderSection480CompaniesAct2006RelatingToDormantCompanies',
        'CompanyEntitledToExemptionUnderSection249A1CompaniesAct1985',
        'CompanyEntitledToExemptionUnderSection249AA1CompaniesAct1985')) AS audit_exempt_old,
    bool_or(cn IN ('OpinionAuditorsOnEntity', 'NameSeniorStatutoryAuditor')) AS audited_old,
    min(json_extract_string(dimensions, '$.AccountsTypeDimension')) FILTER (WHERE cn = 'AccountsType') AS type_new,
    bool_or(cn IN ('AccountsAreInAccordanceWithSpecialProvisionsCompaniesActRelatingToSmallCompanies',
                   'AccountsAreInAccordanceWithSpecialProvisionsInSection4453CompaniesActRelatingToMedium-sizedCompanies',
                   'BusinessReportName')
            AND value ILIKE '%abbreviated%') AS abbreviated_old,
    min(json_extract_string(dimensions, '$.AccountingStandardsDimension')) FILTER (WHERE cn = 'AccountingStandardsApplied') AS std_new,
    bool_or((cn IN ('AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSSE',
                    'AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSSE05',
                    'AccountsPreparedUnderHistoricalCostConventionInAccordanceWithFRSE') AND affirmed)
            OR cn = 'FRSSEVersionDate') AS frsse_old,
    bool_or(lv IN ('true', '1')) FILTER (WHERE cn = 'EntityDormantTruefalse') AS dormant_new,
    bool_or(lv IN ('true', '1')) FILTER (WHERE cn = 'EntityDormant') AS dormant_ed,
    bool_or(cn = 'CompanyDormant' AND affirmed) AS cd_flag,
    bool_or(cn = 'CompanyNotDormant' AND affirmed) AS cnd_flag,
    bool_or(cn = 'EntityTradingStatus') AS ts_present,
    min(json_extract_string(dimensions, '$.EntityTradingStatusDimension')) FILTER (WHERE cn = 'EntityTradingStatus') AS ts_dim,
    bool_or(lv IN ('true', '1')) FILTER (WHERE cn = 'EntityTrading') AS entity_trading,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'EntityCurrentLegalOrRegisteredName') AS name_new,
    arg_min(value, coalesce(dimensions, '')) FILTER (WHERE cn = 'EntityCurrentLegalName') AS name_old,
    min(json_extract_string(dimensions, '$.LegalFormEntityDimension')) FILTER (WHERE cn = 'LegalFormEntity') AS lf_new,
    min(value) FILTER (WHERE cn = 'LegalFormOfEntity') AS lf_old
  FROM g GROUP BY archive, source_file
)
SELECT archive, source_file,
       software,
       coalesce(acc_new, acc_old,
                CASE WHEN NOT coalesce(audited_old, false) OR coalesce(audit_exempt_old, false) THEN agent_aa END) AS accountant_name,
       coalesce(aud_new,
                CASE WHEN audited_old AND NOT coalesce(audit_exempt_old, false) THEN agent_aa END) AS auditor_name,
       coalesce(status_new,
                CASE WHEN audit_exempt_old THEN 'AuditExempt' WHEN audited_old THEN 'Audited' END) AS accounts_status,
       coalesce(type_new, CASE WHEN abbreviated_old THEN 'AbbreviatedAccounts' END) AS accounts_type,
       coalesce(std_new, CASE WHEN frsse_old THEN 'FRSSE' END) AS accounting_standard,
       coalesce(dormant_new, dormant_ed,
                CASE WHEN cd_flag AND NOT coalesce(cnd_flag, false) THEN true
                     WHEN cnd_flag AND NOT coalesce(cd_flag, false) THEN false END) AS dormant,
       CASE WHEN ts_present THEN coalesce(ts_dim, 'Trading')
            WHEN entity_trading THEN 'Trading'
            WHEN NOT entity_trading THEN 'NotTrading' END AS trading_status,
       coalesce(name_new, name_old) AS company_name,
       coalesce(lf_new, CASE lf_old WHEN 'Private Limited Company' THEN 'PrivateLimitedCompanyLtd'
                                    WHEN 'Public Limited Company' THEN 'PublicLimitedCompanyPLC' END) AS legal_form
FROM a;

CREATE OR REPLACE TEMPORARY TABLE accounts_pivot AS
SELECT r.archive, r.zip_start, r.zip_end, r.source_file, r.company_number, t.company_name, r.period_start, r.period_end, c.currency,
       p.employees, p.fixed_assets, p.intangible_assets, p.tangible_assets, p.fixed_asset_investments,
       p.current_assets, p.stocks, p.debtors, p.cash, p.creditors_within_one_year, p.net_current_assets,
       p.total_assets_less_current_liabilities, p.creditors_after_one_year, p.provisions, p.net_assets,
       p.equity, p.share_capital, p.retained_earnings, p.turnover, p.gross_profit, p.operating_profit,
       p.profit_before_tax, p.tax_on_profit, p.profit_loss, p.staff_costs,
       t.software, t.accountant_name, t.auditor_name, t.accounts_status,
       t.accounts_type, t.accounting_standard, t.dormant, t.trading_status, t.legal_form
FROM report r
         LEFT JOIN (
    PIVOT (SELECT archive, source_file, col, priority, v FROM cur)
        ON col IN ('employees', 'fixed_assets', 'intangible_assets', 'tangible_assets', 'fixed_asset_investments',
               'current_assets', 'stocks', 'debtors', 'cash', 'creditors_within_one_year', 'net_current_assets',
               'total_assets_less_current_liabilities', 'creditors_after_one_year', 'provisions', 'net_assets',
               'equity', 'share_capital', 'retained_earnings', 'turnover', 'gross_profit', 'operating_profit',
               'profit_before_tax', 'tax_on_profit', 'profit_loss', 'staff_costs')
  USING arg_min(v, priority)
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
