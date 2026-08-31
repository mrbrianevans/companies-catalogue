import { $ } from "bun";
import { s3Client } from "../eventCapture/utils.ts";

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
  const start = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const end = new Date(now.getFullYear(), now.getMonth(), 0);
  return { start, end };
}

function monthlyZipUrl(month: Date) {
  return `https://download.companieshouse.gov.uk/Accounts_Monthly_Data-${MONTH_NAMES[month.getMonth()]}${month.getFullYear()}.zip`;
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
  const outStream = $`ch-xbrl ${url}`;
  const outFile = s3Client.file(key);
  const rawOutput = await outStream.arrayBuffer();
  const compressedOutput = await Bun.zstdCompress(rawOutput);
  await outFile.write(compressedOutput);
  console.timeEnd("Write CSV from XBRL ZIP URL");
}
