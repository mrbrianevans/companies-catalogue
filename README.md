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