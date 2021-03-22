// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import akka.Done
import com.blackfynn.discover.models.{ PublicDataset, PublicDatasetVersion }
import com.blackfynn.discover.notifications.{
  JobDoneNotification,
  PublishNotification,
  ReleaseNotification,
  SQSNotification
}
import com.blackfynn.service.utilities.{ ContextLogger, LogContext }
import io.circe.{ Decoder, Encoder, Json }
import io.circe.syntax._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sns.SnsAsyncClient
import software.amazon.awssdk.services.sns.model.{ PublishRequest }
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient

import java.time.{ LocalDate, OffsetDateTime }
import java.time.temporal.ChronoUnit
import scala.compat.java8.FutureConverters._
import scala.concurrent.{ ExecutionContext, Future }

case class VictorOpsAlert(
  // The alarm name should be unique. VictorOps groups alerts with the same name
  // into the same alarm
  alarmName: String,
  sourceOrganizationId: Int,
  sourceDatasetId: Int,
  publicDatasetId: Int,
  version: Int,
  embargo: Boolean,
  executionArn: Option[String] = None,
  message: Option[String] = None
)

trait VictorOpsClient {

  def sendAlert(
    alert: VictorOpsAlert
  )(implicit
    logContext: LogContext
  ): Future[Done]

  def sendAlert(
    version: PublicDatasetVersion,
    message: JobDoneNotification
  )(implicit
    logContext: LogContext
  ): Future[Done] = {

    val executionArn = message match {
      case _: PublishNotification =>
        version.executionArn
      case _: ReleaseNotification =>
        version.releaseExecutionArn
    }

    // All new jobs should have an ARN, but we need a fallback
    def fallback: Option[String] => String =
      _.getOrElse(s"dataset=${version.datasetId} version=${version.version}")

    val alarmName = message match {
      case _: PublishNotification =>
        s"Discover publish job failed: ${fallback(executionArn)}"
      case _: ReleaseNotification =>
        s"Discover release job failed: ${fallback(executionArn)}"
    }

    sendAlert(
      VictorOpsAlert(
        alarmName = alarmName,
        sourceOrganizationId = message.organizationId,
        sourceDatasetId = message.datasetId,
        publicDatasetId = version.datasetId,
        version = version.version,
        embargo = version.underEmbargo,
        executionArn = executionArn
      )
    )
  }

  def sendFailedToStartReleaseAlert(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    message: String
  )(implicit
    logContext: LogContext
  ): Future[Done] =
    sendAlert(
      VictorOpsAlert(
        alarmName =
          s"Failed to start embargo release for dataset ${version.datasetId} version ${version.version} on ${LocalDate.now}",
        sourceOrganizationId = dataset.sourceOrganizationId,
        sourceDatasetId = dataset.sourceDatasetId,
        publicDatasetId = version.datasetId,
        version = version.version,
        embargo = version.underEmbargo,
        executionArn = None,
        message = Some(message)
      )
    )
}

class VictorOpsSNSClient(
  alertTopic: String,
  region: Region
)(implicit
  ec: ExecutionContext
) extends VictorOpsClient {

  private lazy val client: SnsAsyncClient = SnsAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  private lazy val log = new ContextLogger().context

  def sendAlert(
    alert: VictorOpsAlert
  )(implicit
    logContext: LogContext
  ): Future[Done] = {
    val messageBody = Json
      .obj(
        "AlarmName" -> Json.fromString(alert.alarmName),
        "NewStateValue" -> Json.fromString("ALARM"),
        "StateChangeTime" -> OffsetDateTime.now
          .truncatedTo(ChronoUnit.MILLIS)
          .asJson,
        "SourceOrganizationId" -> Json.fromInt(alert.sourceOrganizationId),
        "SourceDatasetId" -> Json.fromInt(alert.sourceDatasetId),
        "PublicDatasetId" -> Json.fromInt(alert.publicDatasetId),
        "PublicDatasetVersion" -> Json.fromInt(alert.version),
        "ExecutionArn" -> alert.executionArn.asJson,
        "Embargo" -> alert.embargo.asJson,
        "state_message" -> alert.message.asJson
      )
      .noSpaces

    log.error(s"Sending alert: $messageBody")

    val request = PublishRequest
      .builder()
      .topicArn(alertTopic)
      .message(messageBody)
      .build()

    client.publish(request).toScala.map(_ => Done)
  }
}
