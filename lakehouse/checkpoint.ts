// This is to clean up the lakehouse. must not be run at the same time as other processes which modify the lakehouse.

import { saveAndCloseLakehouse, setupLakehouseConnection } from "./connection.js";
import { executeSql } from "./utils.ts";

// destructive and non-destructive operations are split

async function main() {
  console.log("Checkpointing lakehouse");
  const { connection } = await setupLakehouseConnection();
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

  // operations that destroy old parquet files
  console.time("destructive operations");
  await executeSql(
    connection,
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
  await saveAndCloseLakehouse({ connection });
}

await main();
