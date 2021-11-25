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
  - sync_documents() - gets document metadata from registry and saves to DB (table: document)
- reload()
  - Gets documents to download from DB
  - Downloads docs from publisher's URL, saves to Blob storage, updates DB
