-- duckdb script to:
  -- 1) download latest snapshot from lakehouse table to a local_db table
  -- 2) update the local_db table with any new events since the last snapshot
  -- 3) replace the lakehouse table with the local_db table

ATTACH 'temp.db' AS local_db; -- for handling larger than memory tables.


CREATE
OR REPLACE TABLE local_db.snapshot AS
(SELECT DISTINCT * FROM snapshot);


ALTER TABLE local_db.snapshot ADD PRIMARY KEY (resource_uri);


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
INSERT OR REPLACE INTO local_db.snapshot
FROM deduped;


DELETE
FROM local_db.snapshot
WHERE event.type = 'deleted';

-- replace the lakehouse table to remove time travel history.

CREATE OR REPLACE TABLE snapshot AS
    FROM local_db.snapshot;
