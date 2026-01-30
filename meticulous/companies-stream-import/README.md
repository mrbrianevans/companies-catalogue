
# Fixing missing events after outage

There was an 11 day outage caused by a bug in the pipeline.py dagster code.

Since Companies House only offer 10 days of event history, it couldn't reconnect where it left off.

This used events from companies.stream parquet storage to fill in the gap.

```sql
COPY (SELECT * EXCLUDE (received, stream, uploaded_day) 
      FROM 's3://companies-stream/stream=companies/resource_kind=company-profile/uploaded_day=2026-01-20/*.parquet' 
    WHERE event.timepoint > 107315293) TO '019bd38d-2ca6-7001-8ca1-fe31b1340a25.json';
```
