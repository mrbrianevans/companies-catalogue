 # Companies Catalogue
 
Cataloging companies house SFTP server bulk data products.

## Crawling the SFTP server

Python script to crawl the SFTP server: `crawl_sftp.py`.
```bash
docker build -t sftp-crawler .
docker run -v ~/.ssh/ch_key:/root/.ssh/ch_key:ro -v ~/projects/companies-catalogue/output:/output -e SFTP_USERNAME=your_username sftp-crawler
```

## Summary of run

Bulk images: 86597 (`/free/bulkimage`)
Non-bulk images: 25089

Files by year produced:
```json
 {
  "2018": 1,
  "2020": 2325,
  "2021": 5025,
  "2022": 4741,
  "2023": 4688,
  "2024": 4935,
  "2025": 3361
}
```

Files by product:
```json
{
  "prod216": 244,
  "prod195": 370,
  "prod183": 36,
  "prod101": 1873,
  "prod199": 12,
  "prod197": 1328,
  "prod214": 530,
  "prod202": 346,
  "prod224": 1,
  "prod212": 1328,
  "prod207": 1327,
  "prod217": 47,
  "prod192": 269,
  "prodNEWNAMES": 1327,
  "prod182": 9,
  "prodSURRENDNAMES": 1324,
  "prod100": 3984,
  "prod198": 1332,
  "prod215": 3691,
  "prod203": 530,
  "prod213": 2656,
  "prod201": 1330,
  "prod223": 1182
}
```

Doc files
```
/free/readme-free.txt
/free/prod101/Prod 101 & 183 - Data items 30 June 2020.doc
/free/prod101/Prod 101 - Daily Directory (post EU exit_changes highlighted).doc
/free/prod199/Prod199_Mortgage_Snapshot.doc
/free/prod197/Liquidation Daily Updates (prod 197) May 2019.docx
/free/prod202/Prod202-Weekly-Gazette-V6.6-ECCTA-changes-June-2024-highlighted.pdf
/free/prod192/Prod 192 - Disqualified Directors - ver 0.6.docx
/free/prod100/Prod 100 & 182 - Data Items 30 June 2020.doc
/free/prod100/Prod 100 - Daily Directory Updates (post EU exit_changes highlighted).doc
/free/prod198/Prod 195  198 - Company Appointments snapshot and daily update (post EU exit_changes highlighted).doc
/free/prod198/archive/.DS_Store
/free/prod201/Mort Daily updates prod 201 Final V9 7_2nd May 2013-ver0.1.doc
/free/prod223/Accounts Bulk Data Product  prod 223  spec-ver0.2.doc
```