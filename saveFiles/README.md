
# Save files

Saves the latest files of all products to the storage bucket based on the metadata summary JSON file.

## Env vars
Copy `.example.env` to `.env` and fill in the values.

## Usage

```bash
docker build -t cc-save-files .
```

```bash
docker run -v companies-catalogue/output:/output --env-file .env cc-save-files
```