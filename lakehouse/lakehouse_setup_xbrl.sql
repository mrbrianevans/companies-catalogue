
--- CH-XBRL
CREATE TABLE IF NOT EXISTS xbrl.ch_xbrl
(company_number VARCHAR, period_start DATE, period_end DATE, concept VARCHAR, "value" VARCHAR, unit VARCHAR, dimensions VARCHAR, taxonomy VARCHAR, source_file VARCHAR, decimals VARCHAR, zip_start DATE, zip_end DATE, csv_name VARCHAR);

-- sorted table
ALTER TABLE xbrl.ch_xbrl SET SORTED BY (zip_start ASC);

