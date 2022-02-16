# IATI Refresher

A Python application which has the responsibility of tracking IATI data from around the Web and refreshing the core IATI software's data stores.

Its responsibilities include:

- Downloads IATI data as presented by the IATI registry
- Store and track changes in the source files
- Validate the source files using the IATI Validator API
- Store and index the normalised data in a compressed and performant Postgresql database.
- Flattens the IATI data into documents which are then added to Solr.

# DB Migrations

For alterations and additions to the database schema, migration files are used. These can be found at https://github.com/IATI/refresher/tree/integrateValidator/src/migrations

Each migration file should have an `upgrade` and a `downgrade` variable, containing the sql required to either upgrade to, or downgrade from, that version. A simple example might be:

```
upgrade = """ALTER TABLE public.refresher
    ADD COLUMN valid boolean;"""

downgrade = """ALTER TABLE public.refresher
    DROP COLUMN valid;"""
```

Before running any task, the Refresher checks the version of the database against the version number found at https://github.com/IATI/refresher/blob/integrateValidator/src/constants/version.py. The `__version__` variable in that file is an object with the version number and the corresponding DB migration number. Of course, not every version will require a DB change, but if it does the Refresher will migrate the database either up or down to match the versions, running each step as it goes. So, an upgrade from migration 2 to 4 will run the `upgrade` sql from migration 3 and then 4. A downgrade from 4 to 2 will run the `downgrade` sql from migration 4 and then 3.

# Local Setup

## Prerequisities

- Python 3
- PostgreSQL (version 12)
  - psycopg2 does not work with v13

## Create Local Database

- Creates a database called `refresher` owned by `refresh`
   `createdb refresher -O refresh`

## launch.json

Setup a `.vscode/launch.json` to run locally with attached debugging like so:

```json
{
  "configurations": [
    {
      "name": "Refresh - Local",
      "type": "python",
      "request": "launch",
      "program": "${workspaceFolder}/src/handler.py",
      "console": "integratedTerminal",
      "args": ["-t", "refresh"],
      "env": {
        "AZURE_STORAGE_CONNECTION_STRING": "",
        "AZURE_STORAGE_CONTAINER_SOURCE": "",
        "SOLR_API_URL": "http://localhost:8983/solr/",
        "DB_USER": "refresh",
        "DB_PASS": "",
        "DB_HOST": "localhost",
        "DB_NAME": "refresher",
        "DB_PORT": "5432",
        "PARALLEL_PROCESSES": "10"
      }
    }
```

## Environment Variables

These are required:

### AZURE_STORAGE_CONNECTION_STRING

- This can be found in the Storage Account > Access Keys or by running `az storage account show-connection-string -g MyResourceGroup -n MyStorageAccount`

### AZURE_STORAGE_CONTAINER_SOURCE

- The string name of your blob container in Azure E.g. fun-blob-1

### DB\_\*

Example for connecting to local db you made above:

- "DB_USER": "refresh",
- "DB_PASS": "",
- "DB_HOST": "localhost",
- "DB_NAME": "refresher",
- "DB_PORT": "5432",

See `src/constants/config.py` for additional required Environment variables per service.

# Refresher Logic

Service Loop (when container starts)

- refresh()
  - sync_publishers() - gets publisher metadata from registry and saves to DB (table: publisher)
    - removes any publishers where `document.last_seen` is from a previous run (so no longer in registry)
  - sync_documents() - gets document metadata from registry and saves to DB (table: document)
    - Checks for `changed_datasets` - `document.id` is same, but `document.hash` has changed
    - Updates DB with all documents
      - If there is a conflict with `document.id`, `hash,url,modified,downloaded,download_error` are updated along with `validation_*`, `lakify_*`, `flatten_*`, and `solrize_*` columns
    - Checks for `stale_datasets` - `document.last_seen` is from a previous run (so no longer in registry)
    - clean_datasets()
      - Removes `stale_datasets` from Activity lake, decided it wasn't worth updating `changed_datasets` from activity lake because filenames are hash of `iati_identifier` so less likely to change.
      - Removes `changed_datasets` and `stale_datasets` from source xml blob container and Solr.
    - Removes `stale_datasets` from DB documents table
- reload(retry_errors)
  - `retry_errors` is True after RETRY_ERRORS_AFTER_LOOP refreshes.
  - Gets documents to download from DB (db.getRefreshDataset)
    - If `retry_errors=true` - `"SELECT id, hash, url FROM document WHERE downloaded is null"`
    - Else - `"SELECT id, hash, url FROM document WHERE downloaded is null AND download_error is null"`
  - Downloads docs from publisher's URL, saves to Blob storage, updates DB
    - download_chunk()
      - If successfully uploaded to Blob - `db.updateFileAsDownloaded`
        `"UPDATE document SET downloaded = %(dt)s, download_error = null WHERE id = %(id)s"`
      - If error occurs `db.updateFileAsDownloadError`
        - Not 200 - `document.download_error` = status code
        - Connection Error, or Charset Issue `document.download_error = 0`
      - If `AzureExceptions.ServiceResponseError` or other Exception
        - Warning logged, DB not updated

# Validator Logic

service_loop() calls main(), then sleeps for 60 seconds

- main()
  - Gets unvalidated documents (db.getUnvalidatedDatasets)

```sql
FROM document
WHERE downloaded is not null AND download_error is null AND (validation is Null OR regenerate_validation_report is True)
```

- process_hash_list()
  - Takes document, downloads from Azure blobs
    - If charset undetectable, breaks out of loop for that document
  - POST's to validator API
  - Updates Validation Request Time in db (db.updateValidationRequestDate)
    - `document.validation_request`
  - If Validator Response status code != 200
    - `400, 413, 422`
      - Log status_code in db (db.updateValidationError)
      - Since "expected" and we move on to save report into db
    - `400 - 499`
      - Log status_code in db, break out of loop
    - `> 500`
      - Log status_code in db, break out of loop
    - `else`
      - warning logged, nothing in db, we continue on
  - If exception
    - Can't download BLOB, then `"UPDATE document SET downloaded = null WHERE id = %(id)s"`, to force re-download
    - Other Exception, log message, no change to DB
  - Save report into DB (db.updateValidationState)
    - If `state` is None (bad report) `"UPDATE document SET validation=null WHERE hash=%s"`
    - If ok, save report into `validation` table, set `document.validation = hash` and `document.regenerate_validation_report = False`

# Flattener Logic

service_loop() calls main(), then sleeps for 60 seconds

- main()
  - Reset unfinished flattens

```sql
UPDATE document
SET flatten_start=null
WHERE flatten_end is null
```

  - Get unflattened (db.getUnflattenedDatasets) - downloaded exists, validated where valid = true, flatten_start = Null, fileType = 'iati-activities'

```sql
FROM document as doc
LEFT JOIN validation as val ON doc.validation = val.document_hash
WHERE doc.downloaded is not null
AND doc.flatten_start is Null
AND val.valid = true
AND val.report ->> 'fileType' = 'iati-activities'
```

  - process_hash_list()
    - If prior_error = 422, 400, 413, break out of loop for this file
    - Start flatten in db (db.startFlatten)

```sql
UPDATE document
SET flatten_start = %(now)s, flatten_api_error = null
WHERE id = %(doc_id)s
```

 - Download source XML from Azure blobs
      - If charset error, breaks out of loop for file
    - POST's to flattener API
    - Update Solrize start `"UPDATE document SET solrize_start=%(dt)s WHERE hash=%(hash)s"`
    - If status code != 200
      - `404` - update DB `document.flatten_api_error`, pause 1min, continue loop
      - `400 - 499` - update DB `document.flatten_api_error`, break out of loop
      - `500 +` - update DB `document.flatten_api_error`, break out of loop
      - else - log warning, continue
    - If exception
      - Can't download BLOB, then `"UPDATE document SET downloaded = null WHERE id = %(id)s"`, to force re-download
      - Other Exception, log message, no change to DB

# Lakify

service_loop() calls main(), then sleeps for 60 seconds

- main()
  - Reset unfinished lakifies

```sql
UPDATE document
SET lakify_start=null
WHERE lakify_end is null
AND lakify_error is not null
```

  - Get unlakified documents

```sql
FROM document as doc
LEFT JOIN validation as val ON doc.validation = val.document_hash
WHERE doc.downloaded is not null
AND doc.lakify_start is Null
AND val.valid = true
```

  - process_hash_list()
    - If prior_error = 422, 400, 413, break out of loop for this file
    - Start lakify in DB

```sql
UPDATE document
SET lakify_start = %(now)s, lakify_error = null
WHERE id = %(doc_id)s
```

  - Download source XML from Azure blobs
  - Breaks into individual activities
  - If there is an activity, create hash of iati-identifier and set that as filename
  - Save that file to Azure Blobs activity lake
  - If Exception
    - `etree.XMLSyntaxError, etree.SerialisationError`
      - Log warning, log error to DB
    - Other Exception
      - Log error, log error to DB
  - complete lakify in db (db.completeLakify)

```sql
UPDATE document
SET lakify_end = %(now)s, lakify_error = null
WHERE id = %(doc_id)s
```