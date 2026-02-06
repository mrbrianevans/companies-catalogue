// This is to clean up the lakehouse. must not be run at the same time as other processes which modify the lakehouse.

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

const lakeBucket = new S3Client({bucket: process.env.LAKE_BUCKET})

async function main() {
    console.log('Checkpointing lakehouse')
    const {connection, tempDbFile, remoteCataloguePath} = await setupLakehouseConnection()
    await connection.run(`USE lakehouse;`)

    console.time('checkpoint')
    await connection.run(`CHECKPOINT;`)
    console.timeEnd('checkpoint')

    await saveAndCloseLakehouse({connection, tempDbFile, remoteCataloguePath})
}

await main()