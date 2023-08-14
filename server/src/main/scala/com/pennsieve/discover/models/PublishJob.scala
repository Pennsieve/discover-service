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
    * Contributors are therefore encoded as a string rather than a collection fo objects
    */
  implicit val encoder: Encoder[PublishJob] = Encoder.forProduct22(
    "organization_id",
    "organization_node_id",
    "organization_name",
    "dataset_id",
    "dataset_node_id",
    "published_dataset_id",
    "user_id",
    "user_node_id",
    "user_first_name",
    "user_last_name",
    "user_orcid",
    "s3_bucket",
    "s3_pgdump_key",
    "s3_publish_key",
    "version",
    "doi",
    "contributors",
    "collections",
    "external_publications",
    "publish_bucket",
    "embargo_bucket",
    "workflow_id"
  )(
    j =>
      (
        j.organizationId.toString,
        j.organizationNodeId,
        j.organizationName,
        j.datasetId.toString,
        j.datasetNodeId,
        j.publishedDatasetId.toString,
        j.userId.toString,
        j.userNodeId,
        j.userFirstName,
        j.userLastName,
        j.userOrcid,
        j.s3Bucket.value,
        j.s3PgdumpKey.value,
        j.s3PublishKey.value,
        j.version.toString,
        j.doi,
        j.contributors.asJson.noSpaces,
        j.collections.asJson.noSpaces,
        j.externalPublications.asJson.noSpaces,
        j.publishBucket.value,
        j.embargoBucket.value,
        j.workflowId.toString
      )
  )
}
