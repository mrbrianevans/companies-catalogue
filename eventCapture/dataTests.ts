import { connection } from "../historicalEvents/duckdbConnection.ts";
import { streams } from "../lakehouse/utils.ts";

const sinkBucket = process.env.SINK_BUCKET;

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }

  const latestFileAge = await connection.runAndReadAll(`
        WITH latest_file AS (
            SELECT 
                file, 
                regexp_extract(file, '/([^/]+).json.gz$', 1) as extracted_uuid,
                uuid_extract_timestamp(extracted_uuid::UUID) as timestamp,
                age(current_timestamp, timestamp) as file_age,
                file_age::varchar as file_age_str,
                timestamp::varchar as timestamp_str,
                file_age > INTERVAL '24' hour as older_than_a_day
            FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
            ORDER BY file DESC
            LIMIT 1
        ) 
        SELECT * 
        FROM latest_file
        WHERE older_than_a_day
        ;
    `);

  const latestFileIsOld = latestFileAge.getRowObjects().length > 0;
  console.log("Latest file is old:", latestFileIsOld);
  if (latestFileIsOld) {
    throw new Error("Latest file is older than 24 hours");
  }

  const latestFile = await connection.runAndReadAll(`
        SELECT
            file
        FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
        ORDER BY file DESC
        LIMIT 1
        ;
    `);

  const latestFileName = latestFile.getRowObjects()[0].file as string;
  console.log("Latest file:", latestFileName);

  const latestTimepointsRes = await connection.runAndReadAll(`
    SELECT MIN(event.timepoint) as min_timepoint,
           MAX(event.timepoint) as max_timepoint,
           MIN(event.published_at) as min_published_at,
           MAX(event.published_at) as max_published_at,
           COUNT(*) as count,
           current_timestamp - interval '24' hour as yesterday
    FROM read_json('${latestFileName}');
    `);
  const latestTimepoints = latestTimepointsRes.getRowObjects()[0];
  console.log("Latest file stats:", latestTimepoints);

  if (
    new Date(latestTimepoints.max_published_at as string) <
    new Date(latestTimepoints.yesterday as string)
  ) {
    throw new Error("Latest published_at is too old");
  }

  /*
  Other things to check:
   - are all the events in order
   - does the count of events match the max - min timepoint
   - scan last 5 files and check for duplicate or malformed events
   - check there are no gaps in the last 5 files
   - check that the uuid timestamp roughly matches the published_at's
   - check the resource type is what we expect for the stream
   */
}

await main(process.argv[2]);
