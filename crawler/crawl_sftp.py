import paramiko
import json
import os
import sqlite3
from datetime import datetime
import time
import sys
import re

def crawl_sftp(host, port, username, key_path, base_path='/', db_path='sftp_catalogue.db', jsonl_path='sftp_file_metadata_catalogue.jsonl'):
    # Set up SFTP
    transport = paramiko.Transport((host, port))
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Set up SQLite database (persist on disk)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE,
            size_bytes INTEGER,
            last_modified TEXT
        )
    """)
    conn.commit()

    # Open JSONL file for appending
    jsonl_file = open(jsonl_path, "a", encoding="utf-8")

    def get_latest_date_from_db():
        # pick up from latest crawled directories
        try:
            cursor.execute("""SELECT SUBSTR(path, INSTR(path, 'prod'), INSTR(path, '/20')-INSTR(path, 'prod')) AS product,
                                     MAX(SUBSTR(path, 0, INSTR(path, '/20') + 11)) AS latest
                              FROM files
                              WHERE path LIKE '/free/prod%/20%/%/%/%'
                              GROUP BY 1;""")
            rows = cursor.fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception:
            return {}

    latest_crawled_dates = get_latest_date_from_db()
    print(f"Latest crawled dates: {latest_crawled_dates}")

    def save_file_metadata(full_path, size, mtime):
        record = {
            'path': full_path,
            'size_bytes': size,
            'last_modified': datetime.fromtimestamp(mtime).isoformat()
        }

        # Insert into SQLite (ignore if already exists)
        cursor.execute("""
            INSERT OR IGNORE INTO files (path, size_bytes, last_modified)
            VALUES (?, ?, ?)
        """, (record['path'], record['size_bytes'], record['last_modified']))
        conn.commit()

        # Append to JSONL
        jsonl_file.write(json.dumps(record) + "\n")
        jsonl_file.flush()  # ensures resilience in case of crash

    def recurse(dir_path):
        for entry in sftp.listdir_attr(dir_path):
            if entry.filename == 'bulkimage':
                continue  # skip huge directory. TODO include this. check it works with resumable.
            full_path = dir_path + '/' + entry.filename
            if entry.longname.startswith('d'):  # Directory
                if full_path.startswith('/free/prod'):
                    match = re.match(r'^/free/(prod[A-Z0-9]+)/\d{4}', full_path)
                    prod_code = match.group(1) if match else None
                    latest = latest_crawled_dates.get(prod_code, '')
                    if prod_code and latest and full_path < latest[0:len(full_path)]:
                        print(f"Skipping {full_path} as it is older than latest crawled date {latest}")
                        continue
                recurse(full_path)
            else:  # File
                print("Found path", full_path, entry.st_size)
                save_file_metadata(full_path, entry.st_size, entry.st_mtime)
        time.sleep(1)

    recurse(base_path)

    # Clean up
    jsonl_file.close()
    sftp.close()
    transport.close()
    conn.close()
    print(f"Saved SQLite DB at {db_path} and JSONL at {jsonl_path}")

if __name__ == "__main__":
    username = os.getenv('SFTP_USERNAME')
    key_path = os.getenv('SFTP_KEY')
    db_path: str = sys.argv[1]
    jsonl_path = db_path.replace('.db', '.jsonl')
    print(f"Crawling SFTP to {db_path} and writing JSONL to {jsonl_path}")
    print(f'Start time: {datetime.now()}')
    crawl_sftp(
        host='bulk-live.companieshouse.gov.uk',
        port=22,
        username=username,
        key_path=key_path,
        base_path='/free',
        db_path=db_path,
        jsonl_path=jsonl_path
    )
    print(f'Finish time: {datetime.now()}')
