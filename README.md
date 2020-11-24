# IATI Serverless Refresher
Serverless service based on the IATI Better Refresher

# Local setup
```
docker build .
```

# Local testing
```
docker run --env-file=.env IMAGE_NAME
```

# DB Migrations

Database migrations are handled using Alembic - https://alembic.sqlalchemy.org/

Refer to its fine documentation, and then when making migrations change the autogenerated revision name to reflect the version to which it relates, following the convention - ie for version 0.3.1, change it to BR_0_3_1_whatever_message.py

# Environment Variables

These are required:

AZURE_STORAGE_CONNECTION_STRING
AZURE_STORAGE_CONTAINER_SOURCE
DB_CONNECTION_STRING