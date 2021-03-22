#!/usr/bin/env sh

set  -e

if [ "$#" -ne 2 ]; then
  echo "Expected args: [environment] [jumpbox]"
  echo " > sync-elastic.sh dev non-prod"
  exit 1
fi

if [ -z "$AWS_SESSION_TOKEN" ]; then
  echo "Missing AWS session token. Call assume-role first"
  exit 1
fi

environment=$1
jumpbox=$2
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
elastic_host=$(get_param "/$environment/discover-service/discover-elasticsearch-host")
elastic_port=$(get_param "/$environment/discover-service/discover-elasticsearch-port")
public_url=$(get_param "/$environment/discover-service/discover-public-url")
assets_url=$(get_param "/$environment/discover-service/discover-assets-url")

local_postgres_port=$(expr $RANDOM + 1000)

echo "Opening SSH tunnel to $jumpbox..."
ssh -f -o ExitOnForwardFailure=yes -L "$local_postgres_port:$host:5432" $jumpbox sleep 60

export S3_PUBLISH_BUCKET=$publish_bucket
export DISCOVER_PUBLIC_URL=$public_url
export DISCOVER_ASSETS_URL=$assets_url
export DISCOVER_POSTGRES_HOST=localhost
export DISCOVER_POSTGRES_PORT=$local_postgres_port
export DISCOVER_POSTGRES_USER=$user
export DISCOVER_POSTGRES_PASSWORD=$password
export DISCOVER_ELASTICSEARCH_HOST=$elastic_host
export DISCOVER_ELASTICSEARCH_PORT=$elastic_port
sbt syncElasticSearch/run
