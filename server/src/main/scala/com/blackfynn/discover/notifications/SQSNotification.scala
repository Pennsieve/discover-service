// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.discover.models.S3Bucket
import com.pennsieve.models.PublishStatus
import io.circe._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }
import enumeratum.EnumEntry.UpperSnakecase
import enumeratum._

/**
  * Types of SQS notifications
  */
sealed trait SQSNotificationType extends EnumEntry with UpperSnakecase

object SQSNotificationType
    extends Enum[SQSNotificationType]
    with CirceEnum[SQSNotificationType] {

  val values = findValues

  case object RELEASE extends SQSNotificationType
  case object PUBLISH extends SQSNotificationType
  case object SCAN_FOR_RELEASE extends SQSNotificationType
}

import SQSNotificationType._

/**
  * Generic job type that Discover reads from the SQS queue.
  *
  * As with PublishJob, all integer values must be decoded from strings because
  * Fargate task definitions can only operate on JSON strings.
  */
sealed trait SQSNotification

/**
  * Notification subtype used for successful publish and release jobs.
  */
sealed trait JobDoneNotification extends SQSNotification {
  val organizationId: Int
  val datasetId: Int
  val version: Int

  def success: Boolean
  val error: Option[String]
}

object SQSNotification {

  implicit val decoder: Decoder[SQSNotification] =
    new Decoder[SQSNotification] {
      final def apply(c: HCursor): Decoder.Result[SQSNotification] =
        for {
          jobType <- c.downField("job_type").as[Option[SQSNotificationType]]

          // Backwards-compatible: old discover-publish does not set job_type
          // TODO: set in job and remove this
          notification <- jobType match {
            case None | Some(PUBLISH) => c.as[PublishNotification]
            case Some(RELEASE) => c.as[ReleaseNotification]
            case Some(SCAN_FOR_RELEASE) => c.as[ScanForReleaseNotification]
            case _ =>
              Left(
                DecodingFailure(
                  s"Could not recognize job type $jobType",
                  c.history
                )
              )
          }
        } yield notification
    }

  implicit val encoder: Encoder[SQSNotification] = _ match {
    case n: PublishNotification => n.asJson
    case n: ReleaseNotification => n.asJson
    case n: ScanForReleaseNotification => n.asJson
  }
}

/**
  * Notification sent by `discover-publish` State Machine back to Discover service.
  */
case class PublishNotification(
  organizationId: Int,
  datasetId: Int,
  status: PublishStatus, // TODO: remove this and send "success" boolean from step function
  version: Int,
  error: Option[String] = None
) extends SQSNotification
    with JobDoneNotification {

  def success: Boolean = status == PublishStatus.PublishSucceeded
}

object PublishNotification {

  implicit val encoder: Encoder[PublishNotification] = Encoder.forProduct5(
    "organization_id",
    "dataset_id",
    "status",
    "version",
    "error"
  )(
    j =>
      (
        j.organizationId.toString,
        j.datasetId.toString,
        j.status.asJson,
        j.version.toString,
        j.error.asJson
      )
  )

  implicit val decoder: Decoder[PublishNotification] =
    new Decoder[PublishNotification] {
      final def apply(c: HCursor): Decoder.Result[PublishNotification] =
        for {
          organizationId <- c.downField("organization_id").as[Int]
          datasetId <- c.downField("dataset_id").as[Int]
          version <- c.downField("version").as[Int]
          status <- c.downField("status").as[PublishStatus]
          error <- c.downField("error").as[Option[String]]
        } yield {
          new PublishNotification(
            organizationId,
            datasetId,
            status,
            version,
            error
          )
        }
    }
}

/**
  * Notification sent by `discover-release` State Machine back to Discover service.
  */
case class ReleaseNotification(
  organizationId: Int,
  datasetId: Int,
  version: Int,
  s3Bucket: S3Bucket,
  success: Boolean,
  error: Option[String] = None
) extends SQSNotification
    with JobDoneNotification

object ReleaseNotification {

  implicit val encoder: Encoder[ReleaseNotification] = Encoder.forProduct7(
    "job_type",
    "organization_id",
    "dataset_id",
    "version",
    "s3_bucket",
    "success",
    "error"
  )(
    j =>
      (
        (RELEASE: SQSNotificationType).asJson,
        j.organizationId.toString,
        j.datasetId.toString,
        j.version.toString,
        j.s3Bucket.toString,
        j.success.asJson,
        j.error.asJson
      )
  )

  implicit val decoder: Decoder[ReleaseNotification] =
    new Decoder[ReleaseNotification] {
      final def apply(c: HCursor): Decoder.Result[ReleaseNotification] =
        for {
          organizationId <- c.downField("organization_id").as[Int]
          datasetId <- c.downField("dataset_id").as[Int]
          success <- c.downField("success").as[Boolean]
          s3Bucket <- c.downField("s3_bucket").as[S3Bucket]
          version <- c.downField("version").as[Int]
          error <- c.downField("error").as[Option[String]]
        } yield {
          new ReleaseNotification(
            organizationId,
            datasetId,
            version,
            s3Bucket,
            success,
            error
          )
        }
    }
}

/**
  * Notification periodically sent by Cloudwatch to tell Discover to look for
  * embargoed datasets to release.
  */
case class ScanForReleaseNotification() extends SQSNotification

object ScanForReleaseNotification {

  implicit val encoder: Encoder[ScanForReleaseNotification] =
    Encoder.forProduct1("job_type")(
      j =>
        (
          (SCAN_FOR_RELEASE: SQSNotificationType).asJson
        )
    )

  implicit val decoder: Decoder[ScanForReleaseNotification] =
    new Decoder[ScanForReleaseNotification] {
      final def apply(c: HCursor): Decoder.Result[ScanForReleaseNotification] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == SCAN_FOR_RELEASE) Right(())
          else
            Left(
              DecodingFailure(
                s"Could not recognize job type $jobType",
                c.history
              )
            )
        } yield ScanForReleaseNotification()
    }
}
