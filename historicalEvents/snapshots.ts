// Export snapshots from the datalake for convenient download

import {streams} from "./utils.js";
import {setupLakehouseConnection} from "./connection.js";
import {platform} from "node:os";

const getSchema = (streamPath:string) => streamPath.replaceAll(/[^a-z0-9_]/gi, '_')

const snapshotBucket = process.env.SNAPSHOT_BUCKET

async function main(streamPath: string) {
    if (!streams.includes(streamPath)) {
        console.log('stream', streamPath, 'not in streams list, skipping')
        return
    }
    console.log('Exporting', streamPath, 'snapshots')
    console.time('setup local catalogue')
    const {connection} = await setupLakehouseConnection()
    console.timeEnd('setup local catalogue')

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

    await connection.run(`SET preserve_insertion_order = true;`)

    // export local snapshot to various formats on S3
    const compressionTypes = [{type:'none', extension: ''}, {type:'gzip', extension: '.gz'}, {type:'zstd', extension: '.zst'}]

    for(const compressionType of compressionTypes) {
        console.time('export json'+compressionType.extension)
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.json${compressionType.extension}'
        (FORMAT json, COMPRESSION ${compressionType.type}, ARRAY false, PRESERVE_ORDER true);
        `)
        console.timeEnd('export json'+compressionType.extension)

        console.time('export csv'+compressionType.extension)
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.csv${compressionType.extension}'
        (FORMAT csv, COMPRESSION ${compressionType.type}, PRESERVE_ORDER true, HEADER true);
        `)
        console.timeEnd('export csv'+compressionType.extension)

        console.time('export tsv'+compressionType.extension)
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.tsv${compressionType.extension}'
        (FORMAT csv, COMPRESSION ${compressionType.type}, PRESERVE_ORDER true, HEADER true, DELIMITER '\t');
        `)
        console.timeEnd('export tsv'+compressionType.extension)
    }
        console.time('export parquet')
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.parquet'
        (FORMAT parquet, PRESERVE_ORDER true);
        `)
    console.timeEnd('export parquet')

    if (platform() !== 'win32') {
        console.time('export vortex')
        await connection.run(`
        INSTALL vortex;
        LOAD vortex;
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.vortex'
        (FORMAT vortex, PRESERVE_ORDER true);
        `)
        console.timeEnd('export vortex')
    }
    /*
    TODO:
     - use mongodb to output csv with nested headers
     - output split versions of each format, with 500k items per file
     - change output path to an s3 bucket
     - output a sample of each file format USING SAMPLE 1000
     - add a json manifest to the root dir of bucket with list of files
     - could use variables and prepared statements to reduce repetition?
     */
 connection.closeSync()
}

await main(process.argv[2])