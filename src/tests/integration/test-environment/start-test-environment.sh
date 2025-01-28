
source ../unified-pipeline-test-env-setup.env

export AZURE_STORAGE_CONNECTION_STRING

docker compose up -d

sleep 3

echo "Running storage container create --name source"
az storage container create --name source

echo "Running storage container create --name clean"
az storage container create --name clean

echo "Running az storage container create --name lake"
az storage container create --name lake

echo "Running az storage container create --name validator-adhoc"
az storage container create --name validator-adhoc

echo "Running az storage queue create --name publisher-black-flag-remove"
az storage queue create --name publisher-black-flag-remove

docker compose up
