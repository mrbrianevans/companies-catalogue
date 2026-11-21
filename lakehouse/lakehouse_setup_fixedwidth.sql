--- FIXEDWIDTH (prod216 bulk officers product)
CREATE TABLE IF NOT EXISTS fixedwidth.prod216_companies
(
    production_date    DATE    NOT NULL,
    company_number     VARCHAR NOT NULL,
    company_status     VARCHAR,
    number_of_officers INTEGER,
    company_name       VARCHAR
);

CREATE TABLE IF NOT EXISTS fixedwidth.prod216_persons
(
    production_date       DATE    NOT NULL,
    company_number        VARCHAR NOT NULL,
    app_date_origin       VARCHAR,
    appointment_type      VARCHAR,
    person_number         VARCHAR,
    corporate_indicator   BOOLEAN,
    appointment_date      DATE,
    resignation_date      DATE,
    person_postcode       VARCHAR,
    partial_date_of_birth DATE,
    full_date_of_birth    DATE,
    title                 VARCHAR,
    forenames             VARCHAR,
    surname               VARCHAR,
    honours               VARCHAR,
    care_of               VARCHAR,
    po_box                VARCHAR,
    address_line_1        VARCHAR,
    address_line_2        VARCHAR,
    post_town             VARCHAR,
    county                VARCHAR,
    country               VARCHAR,
    occupation            VARCHAR,
    nationality           VARCHAR,
    resident_country      VARCHAR
);
