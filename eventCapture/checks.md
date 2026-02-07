

```sql
CREATE OR REPLACE TABLE checks AS (
SELECT filename, MIN(event.timepoint) AS min, MAX(event.timepoint) AS max, COUNT(*) as count 
FROM 's3://companies-stream-sink/persons-with-significant-control/019aa*.json.gz' 
GROUP BY filename
);
```

```sql
SELECT * EXCLUDE COUNT, count as countDiff, max - min + 1 AS diff, diff = countDiff as correct, countDiff - diff as extra FROM checks WHERE correct = false ORDER BY min ASC;
```

Would benefit from pattern matching `MATCH_RECOGNIZE` when added to duckdb.
https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/match_recognize/#greedy--reluctant-quantifiers