import paramiko
import json
import os
from datetime import datetime
import time

def crawl_sftp(host, port, username, key_path, base_path='/'):
    transport = paramiko.Transport((host, port))
    pkey = paramiko.RSAKey.from_private_key_file(key_path)
    transport.connect(username=username, pkey=pkey)
    sftp = paramiko.SFTPClient.from_transport(transport)

    files_catalogue = []

    def recurse(dir_path):
        for entry in sftp.listdir_attr(dir_path):
            full_path = os.path.join(dir_path, entry.filename)
            if entry.longname.startswith('d'):  # Directory
                recurse(full_path)
            else:  # File
                print('Found path', full_path)
                files_catalogue.append({
                    'path': full_path,
                    'size_bytes': entry.st_size,
                    'last_modified': datetime.fromtimestamp(entry.st_mtime).isoformat()
                })
        time.sleep(2.5)

    recurse(base_path)
    sftp.close()
    transport.close()
    return files_catalogue

username = os.getenv('SFTP_USERNAME')
catalogue = crawl_sftp('bulk-live.companieshouse.gov.uk', 22, username, '/root/.ssh/ch_key', '/free')
with open('/output/sftp_file_metadata_catalogue.json', 'w') as f:
    json.dump(catalogue, f, indent=4)
print('Saved output to /output/sftp_file_metadata_catalogue.json')