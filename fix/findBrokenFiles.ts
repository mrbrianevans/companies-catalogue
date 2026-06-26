import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import {
  classifyBrokenFile,
  downloadFile,
  getUnloadedFiles,
  isValidJsonFile,
} from "./brokenFileUtils.ts";
import { setupLakehouseConnection } from "../lakehouse/connection.ts";
import { streams } from "../lakehouse/utils.ts";

async function analyseBrokenFile(file: string) {
  const tmpDir = await mkdtemp(join(tmpdir(), "findBrokenFiles-"));
  const localPath = join(tmpDir, basename(file).replace(/\.gz$/, ""));
  try {
    await downloadFile(file, localPath);
    return await classifyBrokenFile(file, localPath);
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
}

async function main(streamPath: string) {
  if (!streamPath) {
    throw new Error("No stream name provided. Usage: bun eventCapture/findBrokenFiles.ts [stream]");
  }
  if (!streams.includes(streamPath)) {
    throw new Error(`Invalid stream. Options: ${streams.join(", ")}`);
  }

  const { connection } = await setupLakehouseConnection();
  try {
    console.log("Finding broken files for stream:", streamPath);
    const unloadedFiles = await getUnloadedFiles(connection, streamPath);
    console.log("Unloaded files to inspect:", unloadedFiles.length);

    for (const file of unloadedFiles) {
      process.stdout.write(`Checking ${file}... `);
      const valid = await isValidJsonFile(connection, file);
      if (valid) {
        console.log("ok");
        continue;
      }

      console.log("INVALID");
      const report = await analyseBrokenFile(file);
      const kindLabel =
        report.kind === "broken_at_end" ? "broken only at end of file" : "broken mid file";
      throw new Error(
        `Broken file found: ${report.file}\n` +
          `Breakage type: ${kindLabel}\n` +
          `Line: ${report.lineNumber}\n` +
          `Content: ${report.line}\n` +
          `Error: ${report.error}`,
      );
    }

    console.log("No broken files found among unloaded files.");
  } finally {
    connection.closeSync();
  }
}

await main(process.argv[2]);
