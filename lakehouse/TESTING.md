# Testing

Daily tests run on every stage of the pipeline to ensure all is well and catch issues early.

## Tests bucket

New S3 bucket for storing test results, and previous testing progress (to avoid repeat processing).

## JSON sink tests

 - tests the 5 most recent files
 - checks data freshness (event published in latest 48 hours)
 - checks contiguous timepoints in each file and across all 5 files


## Events lakehouse tests

Since queries are fast on parquet, don't rely on previous results, check the entire table every time.

Check that all rows are distinct events (no duplicate or missing timepoints).

```sql
SELECT COUNT(*) as count,
       COUNT(DISTINCT event.timepoint) as distinct_timepoints,
       MAX(event.timepoint) - MIN(event.timepoint) + 1 as diff_timepoints
FROM events;
```

The three numbers should all match.

Check that the latest `published_at` is within the last 48 hours.

## Lakehouse snapshot

Check that the latest `published_at` is within the last 48 hours.

Check that all rows are distinct resources (no duplicate resource ids).

## Published snapshots

Check that the latest `published_at` is within the last 48 hours.

Check that all rows are distinct resources (no duplicate resource ids).

Download the Companies House official bulk product for each resource type (where available) and compare the data:

- are the total number of entities similar
- join on resource_id and compare some fields. return count of differences.
- Bear in mind the production time may differ so some differences are expected.
