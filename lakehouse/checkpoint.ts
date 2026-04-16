// This is to clean up the lakehouse. must not be run at the same time as other processes which modify the lakehouse.

import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";
import { DuckDBInstance } from "@duckdb/node-api";
import { executeSql } from "./utils.ts";

// destructive and non-destructive operations are split, with saving the catalogue in between to avoid data loss if the last upload fails.

async function main() {
  console.log("Checkpointing lakehouse");
  const { connection, tempDbFile, remoteCataloguePath } = await setupLakehouseConnection();
  await connection.run(`USE lakehouse;`);

  // operations that don't destroy any parquet files on s3
  console.time("non-destructive operations");
  await executeSql(
    connection,
    `
    CALL ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '1 day');
    CALL ducklake_merge_adjacent_files('lakehouse');
    CALL ducklake_rewrite_data_files('lakehouse');
    CALL ducklake_merge_adjacent_files('lakehouse');
    `,
  );
  console.timeEnd("non-destructive operations");

  // save catalogue
  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath, deleteLocal: false });

  // at this point, the old files are not referenced by ducklake at all.
  // the S3 version of the catalogue references rewritten files not scheduled for deletion.

  {
    const newConn = await setupLakehouseConnection(tempDbFile.name);
    // operations that destroy old parquet files
    console.time("destructive operations");
    await executeSql(
      newConn.connection,
      `
    CALL ducklake_cleanup_old_files(
        'lakehouse',
        cleanup_all => true
    );
    CALL ducklake_delete_orphaned_files(
        'lakehouse',
        older_than => now() - INTERVAL '1 week'
    );
    `,
    );
    console.timeEnd("destructive operations");

    // save catalogue again. less important this time.
    await saveAndCloseLakehouse({ ...newConn, deleteLocal: false });
  }
  {
    // run CHECKPOINT on the catalogue database itself.
    const db = await DuckDBInstance.create(":memory:");
    const newConn = await db.connect();
    console.time("checkpoint catalogue");
    // could possibly attach to __ducklake_metadata
    await newConn.run(`
    ATTACH '${tempDbFile.name}' AS lakehouse;
    USE lakehouse;
    CHECKPOINT;
    `);
    console.timeEnd("checkpoint catalogue");
    await saveAndCloseLakehouse({
      connection: newConn,
      tempDbFile,
      remoteCataloguePath,
      deleteLocal: true,
    });
  }
}

await main();
