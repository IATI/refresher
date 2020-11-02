# IATI Serverless Refresher
Serverless service based on the IATI Better Refresher

# Local setup
```
npm install -g serverless
npm install
python3 -m virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
cp mock_refresh_example.json mock_refresh.json # Edit JSON to match S3 configuration
```

# Local testing
```
serverless invoke local -f refresh -p mock_refresh.json
serverless invoke local -f reload -p mock_reload.json
```

# Deployment (AWS)
```
serverless deploy
```