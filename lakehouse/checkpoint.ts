// This is to clean up the lakehouse. must not be run at the same time as other processes which modify the lakehouse.

import { saveAndCloseLakehouse, setupLakehouseConnection } from "../historicalEvents/connection.js";

// destructive and non-destructive operations are split, with saving the catalogue in between to avoid data loss if the last upload fails.

async function main() {
  console.log("Checkpointing lakehouse");
  const { connection, tempDbFile, remoteCataloguePath } = await setupLakehouseConnection();
  await connection.run(`USE lakehouse;`);

  // operations that don't destroy any parquet files on s3
  console.time("non-destructive operations");
  await connection.run(`
    CALL ducklake_expire_snapshots('lakehouse', older_than => now() - INTERVAL '1 day');
    CALL ducklake_merge_adjacent_files('lakehouse');
    CALL ducklake_rewrite_data_files('lakehouse');
    CALL ducklake_merge_adjacent_files('lakehouse');
    `);
  console.timeEnd("non-destructive operations");

  // save catalogue
  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath, detach: false });

  // at this point, the old files are not referenced by ducklake at all.
  // the S3 version of the catalogue references rewritten files not scheduled for deletion.

  // operations that destroy old parquet files
  console.time("destructive operations");
  await connection.run(`
    CALL ducklake_cleanup_old_files(
        'lakehouse',
        cleanup_all => true
    );
    CALL ducklake_delete_orphaned_files(
        'lakehouse',
        cleanup_all => true
    );
    `);
  console.timeEnd("destructive operations");

  // save catalogue again. less important this time.
  await saveAndCloseLakehouse({ connection, tempDbFile, remoteCataloguePath, detach: true });
}

await main();
