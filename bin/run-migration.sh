#!/usr/bin/env sh

set  -e

if [ "$#" -lt 2 ]; then
  echo "Expected args: [environment] [migration] (dry run?)"
  echo " > run-migration.sh dev ImportFiles true"
  echo
  echo "Note: assumes you have a dev/prod SSH alias"
  exit 1
fi

environment=$1
jumpbox=$1
migration=$2
ssm_prefix="/$environment/discover-service/discover-postgres"

get_param() {
  aws ssm get-parameter --with-decryption --output=text --query Parameter.Value --name $1
}

echo "Getting SSM parameters..."

host=$(get_param "$ssm_prefix-host")
user=$(get_param "$ssm_prefix-user")
password=$(get_param "$ssm_prefix-password")
db="discover-postgres"
publish_bucket=$(get_param "/$environment/discover-service/s3-publish-bucket")

local_postgres_port=$(expr $RANDOM + 1000)

echo "Opening SSH tunnel to $jumpbox..."
ssh -f -o ExitOnForwardFailure=yes -L "$local_postgres_port:$host:5432" $jumpbox sleep 60

export S3_PUBLISH_BUCKET=$publish_bucket
export DISCOVER_POSTGRES_HOST=localhost
export DISCOVER_POSTGRES_PORT=$local_postgres_port
export DISCOVER_POSTGRES_USER=$user
export DISCOVER_POSTGRES_PASSWORD=$password
sbt "server/runMain com.blackfynn.discover.scripts.$migration $3"
