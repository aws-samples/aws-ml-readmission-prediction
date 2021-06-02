##!/bin/bash

##AWS Credentials Setup
export ACCESS_KEY=""
export SECRET_KEY=""

## Upload Setup
export KEY_ID=""
export BUCKET_NAME="" 

aws configure set region us-east-1 --profile uploadProfile
aws configure set profile.uploadProfile.aws_access_key_id $ACCESS_KEY
aws configure set profile.uploadProfile.aws_secret_access_key $SECRET_KEY

export AWS_DEFAULT_PROFILE=uploadProfile

YY=$(date +%Y)
MM=$(date +%m)
DD=$(date +%d)


aws s3 cp allergies.csv s3://$BUCKET_NAME/raw-data/allergies/$YY/$MM/$DD/allergies.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp imaging_studies.csv s3://$BUCKET_NAME/raw-data/imaging_studies/$YY/$MM/$DD/imaging_studies.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp payer_transitions.csv s3://$BUCKET_NAME/raw-data/payer_transitions/$YY/$MM/$DD/payer_transitions.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp patients.csv s3://$BUCKET_NAME/raw-data/patients/$YY/$MM/$DD/patients.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp encounters.csv s3://$BUCKET_NAME/raw-data/encounters/$YY/$MM/$DD/encounters.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp conditions.csv s3://$BUCKET_NAME/raw-data/conditions/$YY/$MM/$DD/conditions.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp medications.csv s3://$BUCKET_NAME/raw-data/medications/$YY/$MM/$DD/medications.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp careplans.csv s3://$BUCKET_NAME/raw-data/careplans/$YY/$MM/$DD/careplans.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp observations.csv s3://$BUCKET_NAME/raw-data/observations/$YY/$MM/$DD/observations.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp procedures.csv s3://$BUCKET_NAME/raw-data/procedures/$YY/$MM/$DD/procedures.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp immunizations.csv s3://$BUCKET_NAME/raw-data/immunizations/$YY/$MM/$DD/immunizations.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp organizations.csv s3://$BUCKET_NAME/raw-data/organizations/$YY/$MM/$DD/organizations.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp providers.csv s3://$BUCKET_NAME/raw-data/providers/$YY/$MM/$DD/providers.csv --sse aws:kms --sse-kms-key-id $KEY_ID
aws s3 cp payers.csv s3://$BUCKET_NAME/raw-data/payers/$YY/$MM/$DD/payers.csv --sse aws:kms --sse-kms-key-id $KEY_ID
