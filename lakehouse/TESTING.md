 # Testing
 
Daily tests run on every stage of the pipeline to ensure all is well and catch issues early.

## Tests bucket
New S3 bucket for storing test results, and previous testing progress (to avoid repeat processing).

## JSON sink tests

Narrow down a time range to test to avoid loads of historical events.
```sql
CREATE OR REPLACE TABLE checks AS (
SELECT filename, MIN(event.timepoint) AS min, MAX(event.timepoint) AS max, COUNT(*) as count 
FROM 's3://companies-stream-sink/persons-with-significant-control/019aa*.json.gz' 
GROUP BY filename
);
```

```sql
SELECT * EXCLUDE COUNT, count as countDiff, max - min + 1 AS diff, diff = countDiff as correct, countDiff - diff as extra 
FROM checks WHERE correct = false ORDER BY min ASC;
```

Checks the integrity of each file to ensure number of events matches the difference between max and min timepoints.

Can then also check the integrity of a range of files.
```sql
SELECT min(min) as tmin, max(max) as tmax, sum(count) as tcount, tmax-tmin+1 as diff, tcount-diff as extra FROM checks;
```

Also check that a file has been uploaded within the last 24/48 hours.

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