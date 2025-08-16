 # Companies Catalogue
 
Cataloging companies house SFTP server bulk data products.

## Crawling the SFTP server

Python script to crawl the SFTP server: `crawl_sftp.py`.
```bash
docker build -t sftp-crawler .
docker run -v ~/.ssh/ch_key:/root/.ssh/ch_key:ro -v ~/projects/companies-catalogue/output:/output -e SFTP_USERNAME=your_username sftp-crawler
```