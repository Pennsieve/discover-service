// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.service.utilities.LogContext
import com.typesafe.scalalogging.StrictLogging
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.FutureConverters._
import scala.util.Success

case class PublishStorageSyncMessage(
  organizationId: Int,
  datasetId: Int,
  publicDatasetId: Int,
  publishedBucket: String,
  manifestKey: String,
  publishedS3Prefix: String
)

object PublishStorageSyncMessage {
  implicit val encoder: Encoder[PublishStorageSyncMessage] =
    Encoder.forProduct6(
      "organization_id",
      "dataset_id",
      "public_dataset_id",
      "published_bucket",
      "manifest_key",
      "published_s3_prefix"
    )(
      m =>
        (
          m.organizationId,
          m.datasetId,
          m.publicDatasetId,
          m.publishedBucket,
          m.manifestKey,
          m.publishedS3Prefix
        )
    )
}

object PublishStorageSync {
  val IsEnabledSSMKey = "ecs-s3-storage-cleanup-task-enabled"
}

trait PublishStorageSyncMessenger {
  def queueMessage(
    message: PublishStorageSyncMessage
  )(implicit
    ec: ExecutionContext
  ): Future[String]
}

class SQSPublishStorageSyncMessenger(
  sqsClient: SqsAsyncClient,
  publishStorageSyncQueueUrl: String
) extends PublishStorageSyncMessenger
    with StrictLogging {

  override def queueMessage(
    message: PublishStorageSyncMessage
  )(implicit
    ec: ExecutionContext
  ): Future[String] = {
    logger.info(
      s"publishStorageSync queueing message [publicDatasetId=${message.publicDatasetId}]"
    )

    val request = SendMessageRequest
      .builder()
      .queueUrl(publishStorageSyncQueueUrl)
      .messageBody(message.asJson.noSpaces)
      .build()

    sqsClient
      .sendMessage(request)
      .asScala
      .map(_.messageId())
      .andThen {
        case Success(messageId) =>
          logger.info(
            s"publishStorageSync queued message " +
              s"[messageId=$messageId, publicDatasetId=${message.publicDatasetId}]"
          )
      }
  }
}
