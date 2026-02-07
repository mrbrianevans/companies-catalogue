// Export snapshots from the datalake for convenient download

import {DuckDBConnection, DuckDBInstance} from "@duckdb/node-api";
import {randomUUIDv7, S3Client} from "bun";
import {tmpdir} from 'node:os'
import {streams} from "./utils.js";
const getSchema = (streamPath:string) => streamPath.replaceAll(/[^a-z0-9_]/gi, '_')
async function setupLakehouseConnection() {

    const tmpDbFilepath = tmpdir() + `/${randomUUIDv7()}_catalogue.ducklake`
    const tempDbFile = Bun.file(tmpDbFilepath)
    const db = await DuckDBInstance.create(':memory:');
    const connection = await db.connect();
    await connection.run("SET threads = 1;")
    await connection.run(`
INSTALL httpfs;
LOAD httpfs;
INSTALL ducklake;

CREATE SECRET s3_lake (
    TYPE s3,
    KEY_ID '${process.env.S3_ACCESS_KEY_ID}',
    SECRET '${process.env.S3_SECRET_ACCESS_KEY}',
    REGION '${process.env.S3_REGION}',
    ENDPOINT '${new URL(process.env.S3_ENDPOINT ?? '').host}',
    SCOPE 's3://${process.env.LAKE_BUCKET}'
);

CREATE SECRET lakehouse (
    TYPE ducklake,
    METADATA_PATH '${tempDbFile.name}',
    DATA_PATH 's3://${process.env.LAKE_BUCKET}/'
);
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


const lakeBucket = new S3Client({bucket: process.env.LAKE_BUCKET})

async function main(streamPath: string) {
    if (!streams.includes(streamPath)) {
        console.log('stream', streamPath, 'not in streams list, skipping')
        return
    }
    console.log('Exporting', streamPath, 'snapshots')
    const {connection, tempDbFile, remoteCataloguePath} = await setupLakehouseConnection()
    await connection.run(`USE lakehouse.${getSchema(streamPath)};`)

    const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`)
    const tables = tablesRes.getRowObjects().map(r => r.name as string)
    console.log('tables in lakehouse', tables)

    await connection.run(`ATTACH 'temp.db' as local;`)
    await connection.run(`CREATE SCHEMA IF NOT EXISTS local.${getSchema(streamPath)};`)

    console.time('create local snapshot from lakehouse')
    await connection.run(`
    CREATE OR REPLACE TABLE local.${getSchema(streamPath)}.snapshot AS 
    SELECT * FROM lakehouse.${getSchema(streamPath)}.snapshot;
    `)
    console.timeEnd('create local snapshot from lakehouse')

    // export local snapshot to various formats on S3
    console.time('export json')
    await connection.run(`
    COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
    TO 'exports/${streamPath}.json'
    (FORMAT json, COMPRESSION none, ARRAY false, PRESERVE_ORDER true);
    `)
    console.timeEnd('export json')

    /*
    TODO:
     - output parquet and vortex
     - use mongodb to output csv with nested headers
     - output split versions of each format, with 500k items per file
     - change output path to an s3 bucket
     - output a sample of each file format USING SAMPLE 1000
     - use each compression type of none, gzip, zst and zipfs.
     - add a json manifest to the root dir of bucket with list of files
     - could use variables and prepared statements to reduce repetition?
     */
 connection.closeSync()
}

await main(process.argv[2])