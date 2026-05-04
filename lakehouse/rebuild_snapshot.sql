
CREATE OR REPLACE TABLE snapshot AS
SELECT * FROM events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY resource_uri
    ORDER BY event.timepoint DESC
) = 1 and event.type != 'deleted';
