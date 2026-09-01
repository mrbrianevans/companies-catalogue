import { s3Client } from "../eventCapture/utils.ts";
import { tmpdir } from "node:os";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { createWriteStream } from "node:fs";

const PREFIX = "ch-xbrl/";
const START_DATE = "2026-01-01";
const PAGES = [
  "https://download.companieshouse.gov.uk/en_monthlyaccountsdata.html",
  "https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html",
] as const;
const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
] as const;
const FILE_RE = /(\d{4}-\d{2}-\d{2})--(\d{4}-\d{2}-\d{2})\.csv\.zst$/;
const ZIP_NAME_RE = /Accounts_Monthly_Data-([A-Za-z]+)(?:To([A-Za-z]+))?(\d{4})\.zip$/i;

type ExistingFile = {
  url: string;
  start: string;
  end: string;
  key: string;
};

function isoDate(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function monthIndex(name: string) {
  const i = MONTH_NAMES.findIndex((m) => m.toLowerCase() === name.toLowerCase());
  return i === -1 ? undefined : i;
}

function parseZipUrl(url: string): ExistingFile | null {
  const filename = url.split("/").pop() ?? "";
  const match = filename.match(ZIP_NAME_RE);
  if (!match) return null;
  const startMonth = monthIndex(match[1]);
  const endMonth = match[2] ? monthIndex(match[2]) : startMonth;
  const year = Number(match[3]);
  if (startMonth === undefined || endMonth === undefined) return null;
  const start = isoDate(new Date(year, startMonth, 1));
  const end = isoDate(new Date(year, endMonth + 1, 0));
  return { url, start, end, key: `${PREFIX}${start}--${end}.csv.zst` };
}

async function zipLinksFromPage(page: string) {
  const res = await fetch(page);
  if (!res.ok) throw new Error(`Failed to fetch ${page}: ${res.status} ${res.statusText}`);

  const links: string[] = [];
  await new HTMLRewriter()
    .on('a[href$=".zip"]', {
      element(el) {
        const href = el.getAttribute("href");
        if (href) links.push(new URL(href, page).href);
      },
    })
    .transform(res)
    .blob(); // drain so handlers complete

  return links;
}

async function filesThatExist() {
  const lists = await Promise.all(PAGES.map(zipLinksFromPage));
  return [...new Set(lists.flat())];
}

async function loadedKeys() {
  const files = await s3Client.list({ prefix: PREFIX, maxKeys: 1000 });
  const len = files.keyCount ?? files.contents?.length ?? 0;
  if (len > 999) throw new Error("Too many files in S3 bucket. Add pagination to list files");

  const keys = new Set<string>();
  for (const { key } of files.contents ?? []) {
    if (key?.match(FILE_RE)) keys.add(key);
  }
  return keys;
}

async function ingest({ url, key }: Pick<ExistingFile, 'url'|'key'>) {
  console.log("Fetching", url, "->", key);

  console.time("Parse and write local");
  const tmp = `${tmpdir()}/${crypto.randomUUID()}.csv.zst`;

  const proc = Bun.spawn(["ch-xbrl", url], {
    stdout: "pipe",
    stderr: "inherit",
  });

  await pipeline(
    Readable.fromWeb(proc.stdout.pipeThrough(new CompressionStream("zstd"))),
    createWriteStream(tmp),
  );

  if ((await proc.exited) !== 0) {
    await Bun.file(tmp).delete().catch(() => {});
    throw new Error(`ch-xbrl exited ${proc.exitCode}`);
  }
  console.timeEnd("Parse and write local");
  console.log("Local file", Bun.file(tmp).size, "bytes");

  console.time("S3 upload");
  await s3Client.file(key, { type: "application/zstd" }).write(Bun.file(tmp), {
    partSize: 16 * 1024 * 1024,
    queueSize: 5,
    retry: 3,
  });
  console.timeEnd("S3 upload");
  await Bun.file(tmp).delete();
}

const existingUrls = await filesThatExist();
const byKey = new Map<string, ExistingFile>();
for (const url of existingUrls) {
  const parsed = parseZipUrl(url);
  if (!parsed) {
    console.warn("Skipping unrecognised zip URL", url);
    continue;
  }
  if (parsed.start < START_DATE) continue;
  if (!byKey.has(parsed.key)) byKey.set(parsed.key, parsed);
}

const loaded = await loadedKeys();
const toLoad = [...byKey.values()]
  .filter((file) => !loaded.has(file.key))
  .sort((a, b) => a.start.localeCompare(b.start) || a.end.localeCompare(b.end));

console.log('To Load:', toLoad)

const next = toLoad[0];
if (!next) {
  console.log("No new XBRL monthly files to ingest after", START_DATE);
} else {
  if (toLoad.length > 1) {
    console.log(`${toLoad.length} pending files after ${START_DATE}; ingesting oldest`, next.key);
  }
  await ingest(next);
}
