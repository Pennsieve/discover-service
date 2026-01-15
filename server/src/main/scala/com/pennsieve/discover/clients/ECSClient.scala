// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import com.pennsieve.discover.StorageCleanupTaskConfiguration
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

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

trait ECSClient {

  /**
    * Runs the s3 storage cleanup task as a Fargate task.
    *
    * @param datasetId The public dataset ID
    * @param version The dataset version
    * @param organizationId The source organization ID
    * @param publishSuccess Whether the publish operation was successful
    * @return The RunTaskResponse from ECS
    */
  def runS3StorageCleanupTask(
    datasetId: Int,
    version: Int,
    organizationId: Int,
    publishSuccess: Boolean
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse]
}

class AwsECSClient(
  config: StorageCleanupTaskConfiguration,
  region: Region
) extends ECSClient
    with StrictLogging {

  private lazy val client: EcsAsyncClient = EcsAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  override def runS3StorageCleanupTask(
    datasetId: Int,
    version: Int,
    organizationId: Int,
    publishSuccess: Boolean
  )(implicit
    ec: ExecutionContext
  ): Future[RunTaskResponse] = {

    val environmentOverrides = List(
      KeyValuePair.builder().name("DATASET_ID").value(datasetId.toString).build(),
      KeyValuePair
        .builder()
        .name("DATASET_VERSION")
        .value(version.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("ORGANIZATION_ID")
        .value(organizationId.toString)
        .build(),
      KeyValuePair
        .builder()
        .name("PUBLISH_SUCCESS")
        .value(publishSuccess.toString)
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
          .subnets(config.subnetIds.asJava)
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
      s"Running s3 storage cleanup task for dataset $datasetId version $version (org: $organizationId, success: $publishSuccess)"
    )

    client.runTask(request).toScala.map { response =>
      if (response.failures().isEmpty) {
        logger.info(
          s"Delete task started successfully for dataset $datasetId version $version"
        )
      } else {
        logger.warn(
          s"Delete task had failures for dataset $datasetId version $version: ${response.failures()}"
        )
      }
      response
    }
  }
}
