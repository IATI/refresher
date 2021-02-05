# IATI Refresher
A Python application which has the responsibility of tracking IATI data from around the Web and refreshing the core IATI software's data stores.

Its responsibilities include:

- Downloads IATI data as presented by the IATI registry
- Store and track changes in the source files
- Validate the source files using the IATI Validator API
- Store and index the normalised data in a compressed and performant Postgresql database.

# Local setup
```
docker build .
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

# Environment Variables

These are required:# IATI Serverless Refresher
2
​
3
Serverless service based on the IATI Better Refresher
4
​
5
# Docker
6
​
7
# Local Docker setup
8
​
9
```
10
docker build .
11
```
12
​
13
# Local Docker testing
14
​
15
```
16
docker run --env-file=.env IMAGE_NAME
17
```
18
​
19
# Local Machine Set Up
20
​
21
## Prerequisities
22
​
23
- Python 3
24
- PostgreSQL (version 12)
25
  - psycopg2 does not work with v13
26
​
27
## Create Database
28
​
29
- Creates a database called `refresher` owned by `refresh`
30
  `createdb refresher -O refresh`
31
​
32
## Environment Variables
33
​
34
These are required:
35
​
36
### AZURE_STORAGE_CONNECTION_STRING
37
​
38
- This can be found in the Storage Account > Access Keys or by running `az storage account show-connection-string -g MyResourceGroup -n MyStorageAccount`
39
​
40
### AZURE_STORAGE_CONTAINER_SOURCE
41
​
42
- The string name of your blob container in Azure E.g. fun-blob-1
43
​
44
### DB_CONNECTION_STRING
45


AZURE_STORAGE_CONNECTION_STRING
AZURE_STORAGE_CONTAINER_SOURCE
DB_CONNECTION_STRING
