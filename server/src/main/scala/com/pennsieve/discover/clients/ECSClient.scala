// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import com.pennsieve.discover.StorageCleanupTaskConfiguration
import com.pennsieve.discover.models.DatasetMetadata
import com.pennsieve.models.PublishStatus
import com.typesafe.scalalogging.StrictLogging
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ecs.EcsAsyncClient
import software.amazon.awssdk.services.ecs.model.{
  AssignPublicIp,
  AwsVpcConfiguration,
  ContainerOverride,
  KeyValuePair,
  LaunchType,
  NetworkConfiguration,
  RunTaskRequest,
  RunTaskResponse,
  TaskOverride
}

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

trait ECSClient {

  /**
    * Runs the s3 storage cleanup task as a Fargate task.
    *
    * @param sourceDatasetId The source dataset ID
    * @param publicDatasetId The public dataset ID
    * @param publishedVersionCount the number of dataset versions with a published status
    * @param lastPublishedDate The timestamp when the version was published
    * @param organizationId The source organization ID
    * @param publishStatus The publish status (e.g., PublishSucceeded, EmbargoSucceeded)
    * @param s3Bucket The S3 bucket where the dataset is published
    * @param s3Key The S3 key prefix for the published dataset
    * @return The RunTaskResponse from ECS
    */
  def runS3StorageCleanupTask(
    sourceDatasetId: Int,
    publicDatasetId: Int,
    publishedVersionCount: Int,
    lastPublishedDate: OffsetDateTime,
    organizationId: Int,
    publishStatus: PublishStatus,
    s3Bucket: String,
    s3Key: String
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse]
}

class AwsECSClient(config: StorageCleanupTaskConfiguration, region: Region)
    extends ECSClient
    with StrictLogging {

  private lazy val client: EcsAsyncClient = EcsAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  override def runS3StorageCleanupTask(
    sourceDatasetId: Int,
    publicDatasetId: Int,
    publishedVersionCount: Int,
    lastPublishedDate: OffsetDateTime,
    organizationId: Int,
    publishStatus: PublishStatus,
    s3Bucket: String,
    s3Key: String
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse] = {

    val environmentOverrides = List(
      KeyValuePair
        .builder()
        .name("DATASET_ID")
        .value(sourceDatasetId.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("PUBLIC_DATASET_ID")
        .value(publicDatasetId.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("PUBLISHED_VERSION_COUNT")
        .value(publishedVersionCount.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("LAST_PUBLISHED_DATE")
        .value(lastPublishedDate.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
        .build(),
      KeyValuePair
        .builder()
        .name("ORGANIZATION_ID")
        .value(organizationId.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("PUBLISH_STATUS")
        .value(publishStatus.entryName)
        .build(),
      KeyValuePair.builder().name("PUBLISHED_BUCKET").value(s3Bucket).build(),
      KeyValuePair.builder().name("PUBLISHED_S3_PREFIX").value(s3Key).build(),
      KeyValuePair
        .builder()
        .name("MANIFEST_KEY")
        .value(DatasetMetadata.MANIFEST_FILE)
        .build()
    )

    val containerOverride = ContainerOverride
      .builder()
      .name(config.containerName)
      .environment(environmentOverrides.asJava)
      .build()

    val taskOverride = TaskOverride
      .builder()
      .containerOverrides(containerOverride)
      .build()

    val networkConfiguration = NetworkConfiguration
      .builder()
      .awsvpcConfiguration(
        AwsVpcConfiguration
          .builder()
          .subnets(config.subnetIds.values.asJava)
          .securityGroups(config.securityGroupId)
          .assignPublicIp(AssignPublicIp.DISABLED)
          .build()
      )
      .build()

    val request = RunTaskRequest
      .builder()
      .cluster(config.cluster)
      .taskDefinition(config.taskDefinition)
      .launchType(LaunchType.FARGATE)
      .networkConfiguration(networkConfiguration)
      .overrides(taskOverride)
      .build()

    logger.info(
      s"Running s3 storage cleanup task for publicDatasetId=$publicDatasetId sourceDatasetId=$sourceDatasetId bucket=$s3Bucket key=$s3Key"
    )

    client.runTask(request).toScala.map { response =>
      if (response.failures().isEmpty) {
        logger.info(
          s"Storage cleanup task started successfully for publicDatasetId=$publicDatasetId"
        )
      } else {
        logger.warn(
          s"Storage cleanup task had failures for publicDatasetId=$publicDatasetId: ${response.failures()}"
        )
      }
      response
    }
  }
}
