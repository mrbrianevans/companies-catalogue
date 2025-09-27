 # Companies Catalogue
 
Cataloging companies house SFTP server bulk data products.

## Stages

- Crawl SFTP server (`crawler`)
  - saves a catalogue of all files on the server.
- Summarise catalogue 
- Save latest files of each data product.
  - get file in original format
  - upload to storage bucket
- convert to other formats
  - using a custom Go parser to get CSV
  - using QSV to convert to other formats (JSON, Parquet)
  - upload to storage bucket


## Build and deploy strategy

Deployed as single executable binaries running on a Digital Ocean droplet.

Orchestration in Python.

Each stage should have its own ./build.ps1 script which builds an executable (target ubuntu). 
The executable should take arguments for local input and output files.
These executables are then run by Python on the Droplet.

Python orchestration will manage uploading files to S3.

Some stages will require access to the SSH server (crawler and saveFiles), and access details should be env vars.

```powershell
.\deploy\deploy.ps1
```