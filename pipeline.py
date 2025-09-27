from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime
from pathlib import Path

from dagster import In, Nothing, Out, Output, job, op, repository, schedule

# schedule for 5am every morning

# stage 1: crawler. output sqlite db file.
#  `python3 crawl_sftp.py /tmp/sftp_catalogue.db`

# stage 2: metadataSummary. input sqlite file, output json file. upload json to s3.
#  `./metadataSummary /tmp/sftp_catalogue.db /tmp/metadataSummary.json`

# stage 3: for each product in metadata summary, saveFiles:
#   - download latest files from sftp to local file system
#   - [output] create a prod200.json file based on the product number, info in the metadata summary, and the product files. Also use docs.
#   - upload prod200.json to s3, upload product files to s3 (gzipped)
#   - call fixedwidth on the product files if supported (currently only prod 216 and prod 195).
#   - upload csv versions to s3. (gzipped). upload parquet versions.
#  `./saveFiles /tmp/metadataSummary.json prod200 /tmp/prod200/`



# Dagster job to orchestrate the three stages described in this file's comments.
# Defaults are conservative and assume required CLIs are installed on the host:
# - Python (for crawler)
# - Bun (for metadata summary TypeScript)
# - Go toolchain (for saveFiles)
# Required environment variables for secrets/targets (read by ops or saveFiles):
# - SFTP_USERNAME (required)
# - SFTP_KEY (path to private key for crawler) or SFTP_KEY_PATH (for saveFiles)
# - S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET (required for saveFiles uploads)
# Optional env: S3_ENDPOINT, S3_REGION, SFTP_HOST, SFTP_PORT

PROJECT_ROOT = Path(__file__).resolve().parents[0]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load environment from project root .env for subprocess execution
def _load_root_dotenv() -> dict:
    env: dict[str, str] = {}
    dot = PROJECT_ROOT / ".env"
    if not dot.exists():
        return env
    try:
        for raw in dot.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k:
                env[k] = v
    except Exception:
        # best-effort; if parsing fails we simply return what we got
        pass
    return env

_ROOT_ENV = _load_root_dotenv()

def _build_subprocess_env(extra: dict | None = None) -> dict:
    env = os.environ.copy()
    # Use .env as defaults: do not override already-set env vars
    for k, v in _ROOT_ENV.items():
        if not env.get(k):
            env[k] = v
    if extra:
        env.update(extra)
    return env


def _timestamped_path(base: Path, suffix: str) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return base / f"{suffix}-{ts}"


@op(
    out=Out(str, description="Path to the generated SQLite DB file from the SFTP crawl"),
    tags={"stage": "crawler"},
)
def crawler_op(context, output_dir: str = str(DEFAULT_OUTPUT_DIR)) -> Output[str]:
    """
    Stage 1: Crawl SFTP and write an on-disk SQLite DB plus a JSONL metadata file.

    Invokes: python crawler/crawl_sftp.py <db_path>
    Reads env: SFTP_USERNAME, SFTP_KEY (path to private key file)
    """

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = _timestamped_path(out_dir, "sftp_catalogue").with_suffix(".db")

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "bin" / "crawl_sftp.py"),
        str(db_path),
    ]
    context.log.info(f"Running crawler: {' '.join(cmd)}")
    env = _build_subprocess_env()
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT), env=env)

    jsonl_path = str(db_path).replace(".db", ".jsonl")
    context.log.info(f"Crawler outputs -> db: {db_path}, jsonl: {jsonl_path}")
    return Output(str(db_path), metadata={"jsonl": jsonl_path})


@op(
    ins={"db_path": In(str)},
    out=Out(str, description="Path to generated metadata summary JSON"),
    tags={"stage": "metadataSummary"},
)
def metadata_summary_op(context, db_path: str, output_dir: str = str(DEFAULT_OUTPUT_DIR)) -> str:
    """
    Stage 2: Generate a metadata summary JSON from the SQLite file using Bun/TypeScript.

    Invokes: bun run metadataSummary/summarise.ts <db_path> <output_json>
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = _timestamped_path(out_dir, "sftp_file_metadata_summary").with_suffix(".json")

    cmd = [
        "/root/.bun/bin/bun",
        "run",
        str(PROJECT_ROOT / "bin" / "metadataSummary.ts"),
        str(db_path),
        str(out_json),
    ]
    context.log.info(f"Running metadata summary: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT), env=_build_subprocess_env())

    # TODO: upload metadata summary to S3

    return str(out_json)


@op(
    ins={"summary_path": In(str)},
    out=Out(Nothing),
    tags={"stage": "saveFiles"},
)
def save_files_op(context, summary_path: str, output_dir: str = str(DEFAULT_OUTPUT_DIR)) -> None:
    """
    Stage 3: For each product in the metadata summary, download latest files and docs
    from SFTP to local filesystem and upload artifacts to S3.

    Invokes: go run ./saveFiles (relies on env for configuration)
    Required env: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET, SFTP_USERNAME, SFTP_KEY_PATH
    Optional env: S3_ENDPOINT, S3_REGION, SFTP_HOST, SFTP_PORT
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # TODO: for each product in the metadata summary, call saveFiles metadataSummary.json prod200 /output/prod200
    #  - modify saveFiles to take params and output to the specified directory
    #  - only process one product at a time for now.
    env = _build_subprocess_env()

    cmd = ["./bin/saveFiles", str(summary_path), str(out_dir)]
    context.log.info(f"Running saveFiles: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT), env=env)


@job
def companies_catalogue_job():
    summary = metadata_summary_op(crawler_op())
    save_files_op(summary)


@schedule(cron_schedule="0 5 * * *", job=companies_catalogue_job, execution_timezone="UTC")
def daily_5am_schedule(_context):
    # Use defaults; can be overridden via run config in Dagster UI/launchers
    return {}


@repository
def companies_catalogue_repo():
    return [companies_catalogue_job, daily_5am_schedule]

