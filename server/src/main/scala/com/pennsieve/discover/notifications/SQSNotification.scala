// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.discover.models.S3Bucket
import com.pennsieve.models.PublishStatus
import io.circe._
import io.circe.syntax._
import io.circe.{ Decoder, Encoder }
import enumeratum.EnumEntry.{ Lowercase, UpperSnakecase }
import enumeratum._
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

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
  case object INDEX extends SQSNotificationType
  case object PUSH_DOI extends SQSNotificationType
  case object NOTIFY_API extends SQSNotificationType
  case object STORE_FILES extends SQSNotificationType
  case object S3_REQUEST extends SQSNotificationType
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

sealed trait JobRequestNotification extends SQSNotification {
  val jobType: SQSNotificationType
  val datasetId: Int
  val version: Int
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
            case Some(INDEX) => c.as[IndexDatasetRequest]
            case Some(PUSH_DOI) => c.as[PushDoiRequest]
            case Some(NOTIFY_API) => c.as[NotifyApiRequest]
            case Some(STORE_FILES) => c.as[StoreFilesRequest]
            case Some(S3_REQUEST) => c.as[S3OperationRequest]
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
    case n: IndexDatasetRequest => n.asJson
    case n: PushDoiRequest => n.asJson
    case n: NotifyApiRequest => n.asJson
    case n: StoreFilesRequest => n.asJson
    case n: S3OperationRequest => n.asJson
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
  publishBucket: S3Bucket,
  embargoBucket: S3Bucket,
  success: Boolean,
  error: Option[String] = None
) extends SQSNotification
    with JobDoneNotification

object ReleaseNotification {

  implicit val encoder: Encoder[ReleaseNotification] = Encoder.forProduct8(
    "job_type",
    "organization_id",
    "dataset_id",
    "version",
    "publish_bucket",
    "embargo_bucket",
    "success",
    "error"
  )(
    j =>
      (
        (RELEASE: SQSNotificationType).asJson,
        j.organizationId.toString,
        j.datasetId.toString,
        j.version.toString,
        j.publishBucket.toString,
        j.embargoBucket.toString,
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
          publishBucket <- c.downField("publish_bucket").as[S3Bucket]
          embargoBucket <- c.downField("embargo_bucket").as[S3Bucket]
          version <- c.downField("version").as[Int]
          error <- c.downField("error").as[Option[String]]
        } yield {
          new ReleaseNotification(
            organizationId,
            datasetId,
            version,
            publishBucket,
            embargoBucket,
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

/**
  * Notification that will ask Discover Service to index a dataset
  */
case class IndexDatasetRequest(
  jobType: SQSNotificationType,
  datasetId: Int,
  version: Int
) extends SQSNotification
    with JobRequestNotification

object IndexDatasetRequest {
  implicit val encoder: Encoder[IndexDatasetRequest] =
    Encoder.forProduct3("job_type", "dataset_id", "version")(
      j =>
        (
          (INDEX: SQSNotificationType).asJson,
          j.datasetId.toString,
          j.version.toString
        )
    )

  implicit val decoder: Decoder[IndexDatasetRequest] =
    new Decoder[IndexDatasetRequest] {
      final def apply(c: HCursor): Decoder.Result[IndexDatasetRequest] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == INDEX) Right(())
          else
            Left(
              DecodingFailure(
                s"Did not recognize job type $jobType (expecting $INDEX)",
                c.history
              )
            )
          datasetId <- c.downField("dataset_id").as[Int]
          version <- c.downField("version").as[Int]
        } yield new IndexDatasetRequest(jobType, datasetId, version)
    }
}

/**
  * Notification that will ask Discover Service to publish the DOI
  */
case class PushDoiRequest(
  jobType: SQSNotificationType,
  datasetId: Int,
  version: Int,
  doi: String
) extends SQSNotification
    with JobRequestNotification

object PushDoiRequest {
  implicit val encoder: Encoder[PushDoiRequest] =
    Encoder.forProduct4("job_type", "dataset_id", "version", "doi")(
      j =>
        (
          (PUSH_DOI: SQSNotificationType).asJson,
          j.datasetId.toString,
          j.version.toString,
          j.doi
        )
    )

  implicit val decoder: Decoder[PushDoiRequest] =
    new Decoder[PushDoiRequest] {
      final def apply(c: HCursor): Decoder.Result[PushDoiRequest] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == PUSH_DOI) Right(())
          else
            Left(
              DecodingFailure(
                s"Did not recognize job type $jobType (expecting $PUSH_DOI)",
                c.history
              )
            )
          datasetId <- c.downField("dataset_id").as[Int]
          version <- c.downField("version").as[Int]
          doi <- c.downField("doi").as[String]
        } yield new PushDoiRequest(jobType, datasetId, version, doi)
    }
}

/**
  * Notification that will ask Discover Service to signal the Pennsieve API
  */
case class NotifyApiRequest(
  jobType: SQSNotificationType,
  datasetId: Int,
  version: Int,
  status: PublishStatus
) extends SQSNotification
    with JobRequestNotification

object NotifyApiRequest {
  implicit val encoder: Encoder[NotifyApiRequest] =
    Encoder.forProduct4("job_type", "dataset_id", "version", "status")(
      j =>
        (
          (NOTIFY_API: SQSNotificationType).asJson,
          j.datasetId.toString,
          j.version.toString,
          j.status.toString
        )
    )

  implicit val decoder: Decoder[NotifyApiRequest] =
    new Decoder[NotifyApiRequest] {
      final def apply(c: HCursor): Decoder.Result[NotifyApiRequest] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == NOTIFY_API) Right(())
          else
            Left(
              DecodingFailure(
                s"Did not recognize job type $jobType (expecting $NOTIFY_API)",
                c.history
              )
            )
          datasetId <- c.downField("dataset_id").as[Int]
          version <- c.downField("version").as[Int]
          status <- c.downField("status").as[PublishStatus]
        } yield new NotifyApiRequest(jobType, datasetId, version, status)
    }
}

/**
  * Notification that will ask Discover Service to store the published files
  */
case class StoreFilesRequest(
  jobType: SQSNotificationType,
  datasetId: Int,
  version: Int,
  s3Key: String,
  s3Version: String
) extends SQSNotification
    with JobRequestNotification

object StoreFilesRequest {
  implicit val encoder: Encoder[StoreFilesRequest] =
    Encoder.forProduct5(
      "job_type",
      "dataset_id",
      "version",
      "s3Key",
      "s3Version"
    )(
      j =>
        (
          (STORE_FILES: SQSNotificationType).asJson,
          j.datasetId.toString,
          j.version.toString,
          j.s3Key,
          j.s3Version
        )
    )

  implicit val decoder: Decoder[StoreFilesRequest] =
    new Decoder[StoreFilesRequest] {
      final def apply(c: HCursor): Decoder.Result[StoreFilesRequest] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == STORE_FILES) Right(())
          else
            Left(
              DecodingFailure(
                s"Did not recognize job type $jobType (expecting $STORE_FILES)",
                c.history
              )
            )
          datasetId <- c.downField("dataset_id").as[Int]
          version <- c.downField("version").as[Int]
          s3Key <- c.downField("s3Key").as[String]
          s3Version <- c.downField("s3Version").as[String]
        } yield
          new StoreFilesRequest(jobType, datasetId, version, s3Key, s3Version)
    }
}

case class S3OperationRequest(
  jobType: SQSNotificationType,
  s3Operation: String,
  s3Bucket: String,
  s3Key: String,
  s3Version: Option[String],
  data: Option[String]
) extends SQSNotification

object S3OperationRequest {
  implicit val encoder: Encoder[S3OperationRequest] = Encoder.forProduct6(
    "job_type",
    "s3_operation",
    "s3_bucket",
    "s3_key",
    "s3_version",
    "data"
  )(
    j =>
      (
        (S3_REQUEST: SQSNotificationType).asJson,
        j.s3Operation,
        j.s3Bucket,
        j.s3Key,
        j.s3Version,
        j.data
      )
  )

  implicit val decoder: Decoder[S3OperationRequest] =
    new Decoder[S3OperationRequest] {
      final def apply(c: HCursor): Decoder.Result[S3OperationRequest] =
        for {
          jobType <- c.downField("job_type").as[SQSNotificationType]
          _ <- if (jobType == S3_REQUEST) Right(())
          else {
            Left(
              DecodingFailure(
                s"Did not recognize job type $jobType (expecting $S3_REQUEST)",
                c.history
              )
            )
          }
          s3Operation <- c.downField("s3_operation").as[String]
          s3Bucket <- c.downField("s3_bucket").as[String]
          s3Key <- c.downField("s3_key").as[String]
          s3Version <- c.downField("s3_version").as[Option[String]]
          data <- c.downField("data").as[Option[String]]
        } yield
          new S3OperationRequest(
            jobType,
            s3Operation,
            s3Bucket,
            s3Key,
            s3Version,
            data
          )
    }
}

sealed trait S3OperationStatus extends EnumEntry with UpperSnakecase
object S3OperationStatus
    extends Enum[S3OperationStatus]
    with CirceEnum[S3OperationStatus] {
  val values = findValues

  case object SUCCESS extends S3OperationStatus
  case object FAILURE extends S3OperationStatus
  case object NOOP extends S3OperationStatus
}

case class S3OperationResponse(
  request: S3OperationRequest,
  status: S3OperationStatus,
  message: Option[String],
  data: Option[String]
)

object S3OperationResponse {
  implicit val encoder: Encoder[S3OperationResponse] =
    deriveEncoder[S3OperationResponse]
  implicit val decoder: Decoder[S3OperationResponse] =
    deriveDecoder[S3OperationResponse]
}
