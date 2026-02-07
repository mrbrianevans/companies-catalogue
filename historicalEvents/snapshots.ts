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

    oneFile: {
        for (const compressionType of compressionTypes) {
            console.time('export json' + compressionType.extension)
            await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/${streamPath}.json${compressionType.extension}'
        (FORMAT json, COMPRESSION ${compressionType.type}, ARRAY false, PRESERVE_ORDER true);
        `)
            console.timeEnd('export json' + compressionType.extension)
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
    }

    splitFiles: {
        const fileSizeBytes = 512 * 1024 * 1024
        const formats = ['json', 'parquet']
        for (const compressionType of compressionTypes) {
            console.time('export split json' + compressionType.extension)
            await connection.run(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
            TO 's3://${snapshotBucket}/split/${streamPath}'
            (FORMAT json, OVERWRITE_OR_IGNORE true, COMPRESSION ${compressionType.type}, ARRAY false, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}');
            `)
            console.timeEnd('export split json' + compressionType.extension)
        }
        console.time('export split parquet')
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot ORDER BY resource_uri) 
        TO 's3://${snapshotBucket}/split/${streamPath}'
        (FORMAT parquet, OVERWRITE_OR_IGNORE true, FILE_SIZE_BYTES ${fileSizeBytes}, FILENAME_PATTERN '${streamPath}_part_{i}');
        `)
        console.timeEnd('export split parquet')

    }

    sample: {
        for (const compressionType of compressionTypes) {
            console.time('export sample json' + compressionType.extension)
            await connection.run(`
            COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
            TO 's3://${snapshotBucket}/sample/${streamPath}.json${compressionType.extension}'
            (FORMAT json, COMPRESSION ${compressionType.type}, ARRAY false);
            `)
            console.timeEnd('export sample json' + compressionType.extension)
        }
        console.time('export parquet sample')
        await connection.run(`
        COPY (FROM local.${getSchema(streamPath)}.snapshot USING SAMPLE 1000) 
        TO 's3://${snapshotBucket}/sample/${streamPath}.parquet'
        (FORMAT parquet);
        `)
        console.timeEnd('export parquet sample')
    }

    /*
    TODO:
     - change output path to an s3 bucket
     - add a json manifest to the root dir of bucket with list of files. RETURN_FILES
     - could use variables and prepared statements to reduce repetition?
     */
    /*
    Other formats to consider:
     - CSV via MongoDB (for nested headers)
     - Avro
     - Zip archive compression
     */
 connection.closeSync()
}

await main(process.argv[2])