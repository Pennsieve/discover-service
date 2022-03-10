# Discover Service

Public collection of datasets published from the Pennsieve platform.

- A **dataset** has a one-to-one correspondence with a dataset in the platform
- A **version** of a dataset is a snapshot of the files and metadata (graph) of a dataset from a certain point in time. Each version of a dataset has a unique DOI.
- A **revision** of a version is an update to the non-graph metadata of a dataset. This includes updates to information like the dataset name, description, contributors, and tags, but not the knowledge graph or source files.

Datasets are published in two different ways:

- Typically, datasets are immediately made public and available for download
- Datasets are occasionally put **under embargo** before being made public. Embargoed datasets are visible on Discover, but the files in the dataset are not publicly available.

After a set period of time, embargoed datasets are **released** to the public.

## Releasing

Every merge to `master` pushes a new version of `discover-service-client` to Nexus. The published JAR is versioned with the same Jenkins image tag as the service Docker container.

## ElasticSearch

To synchronize the `ElasticSearch` cluster after changing the `PublicDatasetDTO` or `FileDocument` definitions, run the environment-appropriate `discover-sync-elasticsearch` task that can be found under the migrations tab in jenkins

## Testing

To run the ElasticSearch Docker container for testing, you may need to increase the default memory limit of Docker Desktop to 4Gb. See here for instructions: https://docs.docker.com/docker-for-mac/#advanced

There is an integration test suite that runs against the Athena database in the `non-prod` environment. You must assume a role in the `non-prod` account to run these tests:

    assume-role non-prod admin
    sbt integration:test
