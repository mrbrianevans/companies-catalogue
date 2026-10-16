
--- CH-XBRL
CREATE TABLE IF NOT EXISTS xbrl.accounts
(
    archive             VARCHAR NOT NULL,
    zip_start           DATE,
    zip_end             DATE,
    source_file         VARCHAR NOT NULL,
    company_number      VARCHAR,
    company_name        VARCHAR,
    period_start        DATE,
    period_end          DATE,
    currency            VARCHAR,
    employees           DOUBLE,
    cash                DOUBLE,
    net_assets          DOUBLE,
    equity              DOUBLE,
    net_current_assets  DOUBLE,
    software            VARCHAR,
    accountant_name     VARCHAR,
    auditor_name        VARCHAR,
    accounts_status     VARCHAR,
    accounts_type       VARCHAR,
    accounting_standard VARCHAR,
    dormant             BOOLEAN,
    trading_status      VARCHAR,
    legal_form          VARCHAR
);
-- sorted table
ALTER TABLE xbrl.accounts SET SORTED BY (archive ASC);

