# Charity Commission bulk data extracts

Available from: https://register-of-charities.charitycommission.gov.uk/en/register/full-register-download

**Unique entities** :

- charity
- charity_annual_return_history
- charity_annual_return_parta
- charity_annual_return_partb
- charity_area_of_operation
- charity_classification
- charity_event_history
- charity_governing_document
- charity_other_names
- charity_other_regulators
- charity_policy
- charity_published_report
- charity_trustee

SQL to load a single entity with DuckDB:

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL zipfs FROM community;
LOAD zipfs;

CREATE OR REPLACE TABLE charity AS (
    SELECT * FROM read_json_objects(
        'zip://https://ccewuksprdoneregsadata1.blob.core.windows.net/data/json/publicextract.charity.zip/publicextract.charity.json',
        format = 'array'
    )
);
```
