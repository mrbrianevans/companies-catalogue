
// This is to move events from the lake (.json.gz) to a Ducklake (parquet lakehouse with metadata)
// Ducklake catalog is frozen on S3.

import {DuckDBConnection, DuckDBInstance} from "@duckdb/node-api";
import {randomUUIDv7, S3Client} from "bun";
import {createReadStream, createWriteStream} from 'node:fs'
import {tmpdir} from 'node:os'
import {Readable} from "node:stream";
import {pipeline} from "node:stream/promises";

async function setupLakehouseConnection(){

const tmpDbFilepath = tmpdir() + `/${randomUUIDv7()}_catalogue.ducklake`
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
`)

    const remoteCataloguePath='catalogue.ducklake'
    const catalogueDbFile = lakeBucket.file(remoteCataloguePath)
    if(await catalogueDbFile.exists()) {
        await tempDbFile.write(await catalogueDbFile.bytes())
        console.log('downloaded lakehouse catalogue to', tempDbFile.name)
    }else{
        console.log('no lakehouse catalogue found, creating one.')
    }

    await connection.run(`ATTACH 'ducklake:lakehouse' AS lakehouse (CREATE_IF_NOT_EXISTS true);`)
    await connection.run(`USE lakehouse;`)
    return {connection, tempDbFile,remoteCataloguePath}
}

async function saveAndCloseLakehouse({connection, tempDbFile,remoteCataloguePath}:{connection: DuckDBConnection, tempDbFile: Bun.BunFile, remoteCataloguePath: string}){
    await connection.run(`
    ATTACH ':memory:' AS memory_db;
    USE memory_db;
    `)
    await connection.run('DETACH lakehouse;')
    await lakeBucket.write(remoteCataloguePath, tempDbFile)
    console.log('uploaded lakehouse catalogue back to', remoteCataloguePath)
}

const lakeBucket = new S3Client({bucket: 'companies-stream-lake'})

async function main() {

    const {connection, tempDbFile,remoteCataloguePath} = await setupLakehouseConnection()

    const tablesRes = await connection.runAndReadAll(`SHOW TABLES;`)
    const tables = tablesRes.getRowObjects().map(r=>r.name as string)
    console.log('tables in lakehouse', tables)

    await connection.run(`
    CREATE TABLE IF NOT EXISTS events (
        resource_kind VARCHAR,
        resource_id VARCHAR,
        resource_uri VARCHAR,
        "data" JSON,
        "event" STRUCT(timepoint BIGINT, published_at VARCHAR, "type" VARCHAR)
    )
    `)

    await saveAndCloseLakehouse({connection, tempDbFile,remoteCataloguePath})
}

await main()