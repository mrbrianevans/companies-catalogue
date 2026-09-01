import { s3Client } from "../eventCapture/utils.ts";
import { tmpdir } from "node:os";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { createWriteStream } from "node:fs";

const PREFIX = "ch-xbrl/";
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

function isoDate(d: Date) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function lastFinishedMonth(now = new Date()) {
  const start = new Date(now.getFullYear(), now.getMonth() - 3, 1);
  const end = new Date(now.getFullYear(), now.getMonth()-2, 0);
  return { start, end };
}

function monthlyZipUrl(month: Date) {
  return `https://download.companieshouse.gov.uk/Accounts_Monthly_Data-${MONTH_NAMES[month.getMonth()]}${month.getFullYear()}.zip`;
}

async function filesThatExist(){
  const PAGE = "https://download.companieshouse.gov.uk/en_monthlyaccountsdata.html";
  //TODO: also crawl https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html and union the links

  const res = await fetch(PAGE);
  const links: string[] = [];

  await new HTMLRewriter()
    .on('a[href$=".zip"]', {
      element(el) {
        const href = el.getAttribute("href");
        if (href) links.push(new URL(href, PAGE).href);
      },
    })
    .transform(res)
    .blob(); // drain so handlers complete

  return links
}

async function latestEndDateInBucket() {
  const files = await s3Client.list({ prefix: PREFIX, maxKeys: 1000 });
  const len = files.keyCount ?? files.contents?.length ?? 0;
  if (len > 999) throw new Error("Too many files in S3 bucket. Add pagination to list files");

  let latest: string | undefined;
  for (const { key } of files.contents ?? []) {
    const match = key.match(FILE_RE);
    if (!match) continue;
    const end = match[2];
    if (!latest || end > latest) latest = end;
  }
  return latest;
}

const { start, end } = lastFinishedMonth();
const monthEnd = isoDate(end);
const latest = await latestEndDateInBucket();

if (latest && latest >= monthEnd) {
  console.log("Already have the latest finished month", monthEnd);
} else {
  const url = monthlyZipUrl(start);
  const key = `${PREFIX}${isoDate(start)}--${monthEnd}.csv.zst`;
  console.log("Fetching", url, "->", key);

  console.time("Write CSV from XBRL ZIP URL");
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

  await s3Client.file(key, { type: "application/zstd" }).write(Bun.file(tmp));
  await Bun.file(tmp).delete();
  console.timeEnd("Write CSV from XBRL ZIP URL");
}
