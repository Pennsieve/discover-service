// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.server.definitions.PublishRequest
import com.pennsieve.doi.models.DoiDTO
import com.pennsieve.models.PublishStatus
import cats.implicits._

import io.circe.{ CursorOp, Decoder, DecodingFailure, Encoder, HCursor, Json }
import io.circe.syntax._
import io.circe.parser.decode

/**
  * Job definition that is sent to the AWS State Machine.
  *
  * @param s3Bucket      this is the target bucket. Either a publish or embargo bucket.
  * @param publishBucket this is either the owning organization's custom publish bucket or
  *                      the default publish bucket if the organization does not have a custom
  *                      bucket.
  * @param embargoBucket this is either the owning organization's custom embargo bucket or
  *                      the default embargo bucket if the organization does not have a custom
  *                      bucket.
  * @param expectPrevious true if the publish job can expect a previous version of the dataset to exist in S3, otherwise false.
  */
case class PublishJob(
  organizationId: Int,
  organizationNodeId: String,
  organizationName: String,
  datasetId: Int,
  datasetNodeId: String,
  publishedDatasetId: Int,
  userId: Int,
  userNodeId: String,
  userFirstName: String,
  userLastName: String,
  userOrcid: String,
  s3Bucket: S3Bucket,
  s3PgdumpKey: S3Key.File,
  s3PublishKey: S3Key.Version,
  version: Int,
  doi: String,
  contributors: List[PublicContributor],
  collections: List[PublicCollection],
  externalPublications: List[PublicExternalPublication],
  publishBucket: S3Bucket,
  embargoBucket: S3Bucket,
  expectPrevious: Boolean,
  workflowId: Long
)

object PublishJob {

  def apply(
    publicDataset: PublicDataset,
    version: PublicDatasetVersion,
    request: PublishRequest,
    doi: DoiDTO,
    contributors: List[PublicContributor],
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    publishBucket: S3Bucket,
    embargoBucket: S3Bucket,
    expectPrevious: Boolean,
    workflowId: Long = PublishingWorkflow.Version4
  ): PublishJob = {
    PublishJob(
      organizationId = publicDataset.sourceOrganizationId,
      organizationNodeId = request.organizationNodeId,
      organizationName = request.organizationName,
      datasetId = publicDataset.sourceDatasetId,
      datasetNodeId = request.datasetNodeId,
      publishedDatasetId = publicDataset.id,
      userId = publicDataset.ownerId,
      userNodeId = request.ownerNodeId,
      userFirstName = request.ownerFirstName,
      userLastName = request.ownerLastName,
      userOrcid = request.ownerOrcid,
      s3Bucket = version.s3Bucket,
      s3PgdumpKey = version.s3Key / "dump.sql",
      s3PublishKey = version.s3Key,
      version = version.version,
      doi = doi.doi,
      contributors = contributors,
      collections = collections,
      externalPublications = externalPublications,
      publishBucket = publishBucket,
      embargoBucket = embargoBucket,
      expectPrevious = expectPrevious,
      workflowId = workflowId
    )
  }

  implicit val contributorEncoder: Encoder[PublicContributor] =
    Encoder.forProduct6(
      "id",
      "first_name",
      "middle_initial",
      "last_name",
      "degree",
      "orcid"
    )(c => (c.id, c.firstName, c.middleInitial, c.lastName, c.degree, c.orcid))

  implicit val collectionEncoder: Encoder[PublicCollection] =
    Encoder.forProduct4(
      "name",
      "source_collection_id",
      "dataset_id",
      "version_id"
    )(c => (c.name, c.sourceCollectionId, c.datasetId, c.versionId))

  /**
    * This encoder uses Camelcase for the fields in order to respect the PublishedExternalPublication class definition
    * that it will be decoded into in the Step Function
    */
  implicit val externalPublicationEncoder: Encoder[PublicExternalPublication] =
    Encoder.forProduct2("doi", "relationshipType")(
      p => (p.doi, p.relationshipType)
    )

  /**
    * PublishJob needs a custom encoder because Fargate task definitions expect
    * environment variables to be passed as strings, not integers, and there is no
    * way to transform JSON types in Step Function input/output/result parameters.
    * Contributors are therefore encoded as a string rather than a collection of objects
    */
  implicit val encoder: Encoder[PublishJob] = (j: PublishJob) =>
    Json.obj(
      "organization_id" -> j.organizationId.toString.asJson,
      "organization_node_id" -> j.organizationNodeId.asJson,
      "organization_name" -> j.organizationName.asJson,
      "dataset_id" -> j.datasetId.toString.asJson,
      "dataset_node_id" -> j.datasetNodeId.asJson,
      "published_dataset_id" -> j.publishedDatasetId.toString.asJson,
      "user_id" -> j.userId.toString.asJson,
      "user_node_id" -> j.userNodeId.asJson,
      "user_first_name" -> j.userFirstName.asJson,
      "user_last_name" -> j.userLastName.asJson,
      "user_orcid" -> j.userOrcid.asJson,
      "s3_bucket" -> j.s3Bucket.value.asJson,
      "s3_pgdump_key" -> j.s3PgdumpKey.value.asJson,
      "s3_publish_key" -> j.s3PublishKey.value.asJson,
      "version" -> j.version.toString.asJson,
      "doi" -> j.doi.asJson,
      "contributors" -> j.contributors.asJson.noSpaces.asJson,
      "collections" -> j.collections.asJson.noSpaces.asJson,
      "external_publications" -> j.externalPublications.asJson.noSpaces.asJson,
      "publish_bucket" -> j.publishBucket.value.asJson,
      "embargo_bucket" -> j.embargoBucket.value.asJson,
      "expect_previous" -> j.expectPrevious.toString.asJson,
      "workflow_id" -> j.workflowId.toString.asJson
    )
}
