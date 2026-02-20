-- duckdb script to:
  -- 1) download latest snapshot from lakehouse table to a local table
  -- 2) update the local table with any new events since the last snapshot
  -- 3) replace the lakehouse table with the local table

ATTACH 'temp.db' AS local; -- for handling larger than memory tables.


CREATE
OR REPLACE TABLE local.snapshot AS
FROM snapshot;


ALTER TABLE local.snapshot ADD PRIMARY KEY (resource_uri);


SET VARIABLE latest_timepoint =
  (SELECT COALESCE(MAX(event.timepoint), 0)
   FROM snapshot);

WITH new_events AS
    (SELECT *
     FROM EVENTS
     WHERE event.timepoint > getvariable('latest_timepoint')
    LIMIT 1000000) ,
    latest AS
   (SELECT resource_uri,
    MAX (event.timepoint) AS max_timepoint
FROM new_events
GROUP BY resource_uri),
    deduped AS
    (SELECT e.*
FROM new_events e
    INNER JOIN latest ON e.event.timepoint = latest.max_timepoint)
INSERT
OR
REPLACE INTO local.snapshot
FROM deduped;


DELETE
FROM local.snapshot
WHERE event.type = 'deleted';

-- replace the lakehouse table to remove time travel history.
BEGIN TRANSACTION;


DROP TABLE snapshot;


CREATE TABLE snapshot AS
    FROM local.snapshot;


COMMIT;