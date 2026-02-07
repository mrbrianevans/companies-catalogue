

```sql
CREATE OR REPLACE TABLE checks AS (
SELECT filename, MIN(event.timepoint) AS min, MAX(event.timepoint) AS max, COUNT(*) as count 
FROM 's3://companies-stream-sink/persons-with-significant-control/019a*.json.gz' 
GROUP BY filename
);
```

```sql
SELECT * EXCLUDE COUNT, count - 1 as countDiff, max - min AS diff, diff = countDiff FROM checks ORDER BY min ASC;
```

Would benefit from pattern matching `MATCH_RECOGNIZE` when added to duckdb.
https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/match_recognize/#greedy--reluctant-quantifiers