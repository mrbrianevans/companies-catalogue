import { streams } from "../lakehouse/utils.ts";
import { DuckDBInstance, INTEGER } from "@duckdb/node-api";

const sinkBucket = process.env.SINK_BUCKET;

async function main(streamPath: string) {
  if (!streams.includes(streamPath)) {
    console.log("stream", streamPath, "not in streams list, skipping");
    return;
  }

  const db = await DuckDBInstance.create(":memory:");
  const connection = await db.connect();
  await connection.run(`
INSTALL httpfs;
LOAD httpfs;

CREATE SECRET s3_sink (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? "").host}'
);
`);

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
  const limit = 5;
  const latestFilesRes = await connection.runAndReadAll(
    `
        SELECT
            file
        FROM glob('s3://${sinkBucket}/${streamPath}/*.json.gz')
        ORDER BY file DESC
        LIMIT $limit
        ;
    `,
    { limit },
    { limit: INTEGER },
  );
  const latestFiles = latestFilesRes.getRowObjects().map((f) => f.file as string);
  console.log("Checking most recent", latestFiles.length, "files");

  const latestFileName = latestFiles[0];
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
  const { yesterday, ...latestTimepoints } = latestTimepointsRes.getRowObjects()[0];
  console.log("Latest file stats:", latestTimepoints);

  if (new Date(latestTimepoints.max_published_at as string) < new Date(yesterday.toString())) {
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

  await connection.run(`
  CREATE OR REPLACE TEMPORARY TABLE checks AS (
  SELECT filename, MIN(event.timepoint) AS min, MAX(event.timepoint) AS max, COUNT(*) as count
  FROM read_json([${latestFiles.map((f) => `'${f}'`).join(", ")}])
  GROUP BY filename
  );
  `);

  const problemFilesRes = await connection.runAndReadAll(`
    SELECT * EXCLUDE COUNT, count as countDiff, max - min + 1 AS diff, diff = countDiff as correct, countDiff - diff as extra
    FROM checks WHERE correct = false ORDER BY min ASC;
  `);
  const problemFiles = problemFilesRes.getRowObjects();
  console.log("Problem files:", problemFiles);
  if (problemFiles.length > 0) throw new Error("Problem files found");
  console.log("No problem files found");

  const rangeCorrectRes = await connection.runAndReadAll(
    `SELECT min(min) as tmin, max(max) as tmax, sum(count) as tcount, tmax-tmin+1 as diff, diff = tcount as correct, tcount-diff as extra FROM checks;`,
  );
  const rangeCorrect = rangeCorrectRes.getRowObjects()[0];
  console.log(`Last ${latestFiles.length} files stats:`, rangeCorrect);
  if (!rangeCorrect.correct) throw new Error("Last 5 files count vs diff incorrect");
  console.log(`Last ${latestFiles.length} files are correct and complete`);

  console.log("All passed!");
  connection.closeSync();
}

await main(process.argv[2]);
