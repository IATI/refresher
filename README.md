# IATI Refresher

A Python application which has the responsibility of tracking IATI data from around the Web and refreshing the core IATI software's data stores.

Its responsibilities include:

- Downloads IATI data as presented by the IATI registry
- Store and track changes in the source files
- Validate the source files using the IATI Validator API
- Store and index the normalised data in a compressed and performant Postgresql database.
- Flattens the IATI data into documents which are then added to Solr.

# Local setup

```
docker build . -f refresh-<service>  -t IMAGE_NAME
```

# Local testing

```
docker run --env-file=.env IMAGE_NAME
```

# DB Migrations

For alterations and additions to the database schema, migration files are used. These can be found at https://github.com/IATI/refresher/tree/integrateValidator/src/migrations

Each migration file should have an `upgrade` and a `downgrade` variable, containing the sql required to either upgrade to, or downgrade from, that version. A simple example might be:

```
upgrade = """ALTER TABLE public.refresher
    ADD COLUMN valid boolean;"""

downgrade = """ALTER TABLE public.refresher
    DROP COLUMN valid;"""
```

Before running any task, the Refresher checks the version of the database againsts the version number found at https://github.com/IATI/refresher/blob/integrateValidator/src/constants/version.py. The `__version__` variable in that file is an object with the version number and the corresponding DB migration number. Of course, not every version will require a DB change, but if it does the Refresher will migrate the database either up or down to match the versions, running each step as it goes. So, an upgrade from migration 2 to 4 will run the `upgrade` sql from migration 3 and then 4. A downgrade from 4 to 2 will run the `downgrade` sql from migration 4 and then 3.


# Local Machine Set Up


## Prerequisities

- Python 3

- PostgreSQL (version 12)

   - psycopg2 does not work with v13

## Create Database

- Creates a database called `refresher` owned by `refresh`
   `createdb refresher -O refresh`

## Environment Variables

`cp .example.env .env` - copy blank set

- `AZURE_STORAGE_CONNECTION_STRING` - This can be found in the Storage Account > Access Keys or by running `az storage account show-connection-string -g MyResourceGroup -n MyStorageAccount`
- `AZURE_STORAGE_CONTAINER_SOURCE` - The string name of your blob container in Azure E.g. fun-blob-1
- `DB_USER` - postgres db user - set the same as database.env for docker compose testing
- `DB_HOST` - postgres host - set to postgres for docker compose testing
- `DB_NAME` - database name - set the same as database.env for docker compose testing
- `DB_PASS` - database password - set the same as database.env for docker compose testing
- `DB_PORT` - database port (default 5432)
- `VALIDATOR_API_URL` - url for the validator API
- `VALIDATOR_API_KEY_NAME` - x-functions-key
- `VALIDATOR_API_KEY_VALUE` - API key
- `SOLR_API_URL` - url for SOLR instance, if not using solr need to put a dummy for initialisation

# Docker Compose 

Test locally without setting up a local postgres instance

`docker compose up`

Uses the .env file for environment variables 

## Postgres

`database.env` initialises the postgres db in the Docker container with a database/user

- POSTGRES_DB=refresher
- POSTGRES_USER=refresh
- POSTGRES_PASSWORD=yourpasswordhere
