// metadataSummary.ts
import {SQL} from "bun";
import {lstat, writeFile} from "fs/promises";
import {MetadataSummary, ProductSummary} from "./types";

const FILE_PATH_REGEX = /^\/free\/prod\d+\/\d{4}\/\d{2}\/\d{2}\/[^\/]+\.[^\/]+$/;

interface DocRow {
    path: string;
    product: string|undefined;       // e.g. "prod195"
}

async function fetchDocs(dbPath: string): Promise<DocRow[]>{
    console.log(`Fetching files from ${dbPath}`);
    const stats = await lstat(dbPath)
    console.log(stats.size.toLocaleString(), 'bytes DB file')

    const db = new SQL(`sqlite://${dbPath}?mode=ro`);

    // Extract product and date from the path using SQLite string ops
    const rows = await db<FileRow[]>`
        SELECT path
        FROM files
        WHERE path NOT LIKE '/free/prod%/____/__/__/%'
        AND path NOT LIKE '%/.DS_Store'
        ;
    `;

    return rows.map(row=>{

        const parts = row.path.split("/");
        const product = parts.length >= 4 ? parts[2] : undefined; // eg "prod195"
        return {path: row.path, product}
    })
}


interface FileRow {
    path: string;
    size_bytes: number;
    last_modified: string; // ISO
    product: string;       // e.g. "prod195"
    date: string;          // e.g. "2023-01-03"
}

async function fetchFiles(dbPath: string): Promise<FileRow[]> {
    console.log(`Fetching files from ${dbPath}`);
    const stats = await lstat(dbPath)
    console.log(stats.size.toLocaleString(), 'bytes DB file')

    const db = new SQL(`sqlite://${dbPath}?mode=ro`);

    // Extract product and date from the path using SQLite string ops
    const rows = await db<FileRow[]>`
        SELECT path,
               size_bytes,
               last_modified
        FROM files
        WHERE path LIKE '/free/prod%/____/__/__/%'
    `;

    // Normalize to include a YYYY-MM-DD date field
    return rows
        .filter(row => FILE_PATH_REGEX.test(row.path))
        .map((row) => {
        // path format: /free/prod195/2023/01/03/filename.ext
        const parts = row.path.split("/");
        const product = parts[2]; // "prod195"
        const yyyy = parts[3];
        const mm = parts[4];
        const dd = parts[5];
        const date = `${yyyy}-${mm}-${dd}`;

        return {
            path: row.path, size_bytes: row.size_bytes, last_modified: row.last_modified, product, date,
        };
    });
}

function summarizeProduct(files: FileRow[], docs: DocRow[]): ProductSummary {
    const product = files[0].product;

    // ---- Latest files ----
    // Sort by date, descending
    files.sort((a, b) => (a.date < b.date ? 1 : -1));
    const latestDate = files[0].date;
    const latestGroup = files.filter((f) => f.date === latestDate);
    const latestFiles = latestGroup.map((f) => f.path);
    const latest_last_modified = latestGroup
        .map((f) => f.last_modified)
        .reduce((max, cur) => (max && max > cur ? max : cur), latestGroup[0].last_modified);

    // ---- Average interval days (last year) ----
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);

    const uniqueDates = Array.from(new Set(files
        .filter((f) => new Date(f.date) >= oneYearAgo)
        .map((f) => f.date))).sort();

    let avgIntervalDays: number | null = null;
    if (uniqueDates.length > 1) {
        const intervals: number[] = [];
        for (let i = 1; i < uniqueDates.length; i++) {
            const d1 = new Date(uniqueDates[i - 1]);
            const d2 = new Date(uniqueDates[i]);
            const diffDays = (d2.getTime() - d1.getTime()) / (1000 * 60 * 60 * 24);
            intervals.push(diffDays);
        }
        avgIntervalDays = intervals.reduce((a, b) => a + b, 0) / intervals.length;
    }

    // ---- Average size over last 5 runs ----
    // A "run" = all files for a given date
    const groupedByDate: Record<string, FileRow[]> = {};
    for (const f of files) {
        groupedByDate[f.date] ??= [];
        groupedByDate[f.date].push(f);
    }
    const datesDesc = Object.keys(groupedByDate).sort().reverse();
    const last5 = datesDesc.slice(0, 5);

    const avgSizes: number[] = last5.map((d) => {
        return groupedByDate[d].reduce((sum, f) => sum + f.size_bytes, 0)
    });

    const avgSizeLast5 = avgSizes.length > 0 ? avgSizes.reduce((a, b) => a + b, 0) / avgSizes.length : null;

    return {
        product,
        latest_files: latestFiles,
        latest_date: latestDate,
        latest_last_modified: latest_last_modified,
        avg_interval_days: avgIntervalDays,
        avg_size_last5: avgSizeLast5,
        last5_dates: last5,
        docs: docs.map(doc=>doc.path)
    };
}

async function writeSummary(dbPath: string, outputPath: string) {
    const rows = await fetchFiles(dbPath);
    const docs = await fetchDocs(dbPath)

    // Group rows by product
    const byProduct: Record<string, FileRow[]> = {};
    for (const r of rows) {
        byProduct[r.product] ??= [];
        byProduct[r.product].push(r);
    }

    const summary: ProductSummary[] = Object.values(byProduct).map(files=>summarizeProduct(files, docs.filter(doc=>doc.product === files[0].product)));

    // Top-level metadata and totals
    const generated_at = new Date().toISOString();

    let most_recent_last_modified: string | null = null;
    if (rows.length > 0) {
        // rows[].last_modified is already ISO string; compute the max
        most_recent_last_modified = rows
            .map(r => r.last_modified)
            .reduce((max, cur) => (max && max > cur ? max : cur), rows[0].last_modified);
    }

    const total_avg_size_last5 = summary
        .map(s => s.avg_size_last5 ?? 0)
        .reduce((a, b) => a + b, 0);

    const total_size_bytes = rows
        .map(r => r.size_bytes)
        .reduce((a, b) => a + b, 0);

    const output: MetadataSummary = {
        generated_at,
        most_recent_last_modified,
        total_avg_size_last5,
        total_size_bytes,
        products: summary,
        docs: docs.filter(doc=>!doc.product).map(doc=>doc.path)
    };
    await writeFile(outputPath, JSON.stringify(output, null, 2), "utf-8");
    console.log(`Metadata summary written to ${outputPath}`);
}

if (import.meta.main) {
    const dbPath = process.argv[2] || "/output/sftp_catalogue.db";
    const outputPath = process.argv[3] || "/output/sftp_file_metadata_summary.json";
    writeSummary(dbPath, outputPath).catch((err) => {
        console.error("Error generating metadata summary:", err);
        process.exit(1);
    });
}
