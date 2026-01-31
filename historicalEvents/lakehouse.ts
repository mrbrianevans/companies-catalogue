// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import {DuckDBConnection, DuckDBInstance} from "@duckdb/node-api";
import {randomUUIDv7, S3Client} from "bun";
import {tmpdir} from 'node:os'
import {streams} from "./utils.js";

async function setupLakehouseConnection() {

    const tmpDbFilepath = tmpdir() + `/${randomUUIDv7()}_catalogue.ducklake`
    const tmpDuckdbFilepath = tmpdir() + `/${randomUUIDv7()}_catalogue.duckdb`
    const tempDbFile = Bun.file(tmpDbFilepath)
    const db = await DuckDBInstance.create(':memory:');
    const connection = await db.connect();
    await connection.run(`
INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;

CREATE SECRET s3_sink (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? '').host}',
    SCOPE 's3://companies-stream-sink'
);

CREATE SECRET s3_lake (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? '').host}',
    SCOPE 's3://companies-stream-lake'
);

CREATE SECRET lakehouse (
    TYPE ducklake,
    METADATA_PATH '${tempDbFile.name}',
    DATA_PATH 's3://companies-stream-lake/'
);
ATTACH '${tmpDuckdbFilepath}' AS local_db;
`)

    const remoteCataloguePath = 'catalogue.ducklake'
    const catalogueDbFile = lakeBucket.file(remoteCataloguePath)
    if (await catalogueDbFile.exists()) {
        await tempDbFile.write(await catalogueDbFile.bytes())
        console.log('downloaded lakehouse catalogue to', tempDbFile.name)
    } else {
        console.log('no lakehouse catalogue found, creating one.')
    }

    await connection.run(`ATTACH 'ducklake:lakehouse' AS lakehouse (CREATE_IF_NOT_EXISTS true);`)
    await connection.run(`USE lakehouse;`)
    return {connection, tempDbFile, remoteCataloguePath}
}

async function saveAndCloseLakehouse({connection, tempDbFile, remoteCataloguePath}: {
    connection: DuckDBConnection,
    tempDbFile: Bun.BunFile,
    remoteCataloguePath: string
}) {
    await connection.run(`
    ATTACH ':memory:' AS memory_db;
    USE memory_db;
    `)
    await connection.run('DETACH lakehouse;')
    await lakeBucket.write(remoteCataloguePath, tempDbFile)
    console.log('uploaded lakehouse catalogue back to', remoteCataloguePath)
}

const lakeBucket = new S3Client({bucket: 'companies-stream-lake'})

async function main(streamPath: string) {
    if (!streams.includes(streamPath)) {
        console.log('stream', streamPath, 'not in streams list, skipping')
        return
    }
    console.log('Loading', streamPath, 'into lakehouse')
    const {connection, tempDbFile, remoteCataloguePath} = await setupLakehouseConnection()

    const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`)
    const tables = tablesRes.getRowObjects().map(r => r.name as string)
    console.log('tables in lakehouse', tables)

    await connection.run(`
        CREATE TABLE IF NOT EXISTS events
        (
            stream VARCHAR,
            resource_kind VARCHAR,
            resource_id VARCHAR,
            resource_uri VARCHAR,
            "data" JSON,
            "event" STRUCT(timepoint BIGINT, published_at VARCHAR, "type" VARCHAR)
        );
        CREATE TABLE IF NOT EXISTS snapshot AS FROM events WITH NO DATA;
    `)

    await connection.run(`CREATE SCHEMA IF NOT EXISTS cc_metadata;`)
    await connection.run(`CREATE TABLE IF NOT EXISTS cc_metadata.loaded_files
                          (
                              file VARCHAR
                          )`)
    // find files in S3 that aren't loaded
    console.log('Finding files to load')
    const res = await connection.runAndReadAll(`
        SELECT file
        FROM glob('s3://companies-stream-sink/${streamPath}/*.json.gz')
        WHERE file NOT IN (SELECT file FROM cc_metadata.loaded_files)
        ORDER BY file ASC
        LIMIT 2;
    `)

    const files = res.getRowObjects().map(f => f.file as string)
    if (files.length) {
        console.log('Loading', files.length, 'files into lakehouse', files)


        await connection.run(`CREATE OR REPLACE TABLE local_db.new_events_tmp AS FROM events WITH NO DATA;`)
        await connection.run(`INSERT INTO local_db.new_events_tmp BY NAME
                              SELECT '${streamPath}' as stream, *
                              FROM read_json(${JSON.stringify(files)});`)
        console.log('Loaded', files.length, 'files into new_events_tmp')
        await connection.run('BEGIN TRANSACTION;')
        await connection.runAndReadAll(`
            INSERT INTO events BY NAME
            (FROM local_db.new_events_tmp
             WHERE event.timepoint > (SELECT MAX(event.timepoint) FROM events WHERE stream = '${streamPath}')
                );`)
        console.log('Loaded', files.length, 'files into main events table')


        await connection.run(`INSERT INTO cc_metadata.loaded_files
                              VALUES ${files.map(f => `('${f}')`).join(',')};`)
        console.log('Updated loaded_files table with', files.length, 'new files')

        await connection.run('COMMIT;')

        await connection.run('DROP TABLE local_db.new_events_tmp;')
        // await connection.run('CHECKPOINT;') // Do this once a week/month
    }
    await saveAndCloseLakehouse({connection, tempDbFile, remoteCataloguePath})
}

await main(process.argv[2])