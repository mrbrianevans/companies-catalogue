# Summarise catalogue metadata

Creates a summary file containing metadata extracted from the full file catalogue of the server.

- Uses data generated in the crawler step, from the SQLite database.
- Metadata summary contains latest files for each product and summary stats.


## Usage
```bash
docker build -t cc-metadata . 
```

```bash
 docker run -v companies-catalogue/output:/output cc-metadata
```

