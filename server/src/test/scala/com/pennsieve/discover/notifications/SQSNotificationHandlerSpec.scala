// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.alpakka.sqs.MessageAction
import com.pennsieve.discover.ServiceSpecHarness
import com.pennsieve.models.PublishStatus
import com.pennsieve.models.DatasetMetadata._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.clients.{
  MockDoiClient,
  MockPennsieveApiClient,
  MockS3StreamClient,
  MockSearchClient,
  MockVictorOpsClient
}
import com.pennsieve.discover.models.{
  PublicDatasetVersion,
  PublicFile,
  PublishJobOutput,
  PublishingWorkflow,
  S3Bucket
}
import com.pennsieve.discover.db.{
  PublicDatasetVersionsMapper,
  PublicFilesMapper
}
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.discover.server.definitions.DatasetPublishStatus
import com.pennsieve.discover.TestUtilities
import com.pennsieve.doi.client.definitions._
import com.sksamuel.elastic4s.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import software.amazon.awssdk.services.sqs.model.Message
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.util.Calendar
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SQSNotificationHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ServiceSpecHarness {

  implicit private val system: ActorSystem = ActorSystem("discover-service")
  implicit private val executionContext: ExecutionContext = system.dispatcher

  lazy val notificationHandler = new SQSNotificationHandler(
    ports,
    config.sqs.region,
    config.sqs.queueUrl,
    config.sqs.parallelism
  )

  /**
    * Run an incoming job done notification through the SQS notification stream.
    */
  def processNotification(notification: SQSNotification): MessageAction = {
    val sqsMessage = Message
      .builder()
      .body(notification.asJson.toString)
      .build()

    Source
      .single(sqsMessage)
      .via(notificationHandler.notificationFlow)
      .runWith(Sink.seq)
      .awaitFinite(10.seconds)
      .head
  }

  "Publish notifications queue handler" should {
    "update publish status as successful" in {

      val datasetName = TestUtilities.randomString()

      val publicDataset =
        TestUtilities.createDataset(ports.db)(name = datasetName)

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val currentYear = Calendar.getInstance().get(Calendar.YEAR)
      val futureYear = currentYear + 5

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.NotPublished,
        doi = doi.doi,
        embargoReleaseDate = Some(LocalDate.of(futureYear, 3, 14))
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            totalSize = 76543
          )
        )

      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        )
      ) shouldBe an[MessageAction.Delete]

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
        )
        .awaitFinite()

      // Should update version info from outputs.json
      inside(publicVersion) {
        case v: PublicDatasetVersion =>
          v.status shouldBe PublishStatus.PublishSucceeded
          v.size shouldBe 76543
          v.fileCount shouldBe 1
          v.readme shouldBe Some(v.s3Key / "readme.md")
          v.banner shouldBe Some(v.s3Key / "banner.jpg")
      }

      val files = ports.db
        .run(
          PublicFilesMapper
            .forVersion(publicVersion)
            .result
        )
        .awaitFinite()

      // Should create database entries for newly published files
      files.length shouldBe 1
      inside(files.head) {
        case f: PublicFile =>
          f.name shouldBe "brain.dcm"
          f.s3Key shouldBe publicVersion.s3Key / "files/brain.dcm"
          f.size shouldBe 15010
          f.fileType shouldBe "DICOM"
          f.sourcePackageId shouldBe Some("N:package:1")
      }

      // Should delete the outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .publishResults shouldBe empty

      ports.pennsieveApiClient
        .asInstanceOf[MockPennsieveApiClient]
        .publishCompleteRequests shouldBe List(
        (
          DatasetPublishStatus(
            publicDataset.name,
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId,
            Some(publicDataset.id),
            1,
            PublishStatus.PublishSucceeded,
            Some(publicVersion.createdAt),
            workflowId = PublishingWorkflow.Version4
          ),
          None
        )
      )

      val actualDoi: DoiDTO = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .dois(doi.doi)
      actualDoi.title shouldBe Some(publicDataset.name)
      actualDoi.creators shouldBe Some(List())
      actualDoi.publicationYear shouldBe Some(futureYear)
      actualDoi.url shouldBe Some(
        s"https://discover.pennsieve.org/datasets/${publicDataset.id}/version/${publicDatasetV1.version}"
      )

      val (
        indexedVersion,
        indexedRevision,
        indexedSponsorship,
        indexedFiles,
        indexedRecords
      ) =
        ports.searchClient
          .asInstanceOf[MockSearchClient]
          .indexedDatasets(publicDataset.id)

      indexedVersion.version shouldBe publicDatasetV1.version
      indexedRevision shouldBe None
      indexedSponsorship shouldBe None

      // From defaults in MockS3StreamClient
      indexedFiles.length shouldBe 1
      indexedRecords.length shouldBe 1
    }

    "update publish status as failed" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.NotPublished,
        doi = doi.doi
      )

      val publishNotification = PublishNotification(
        publicDataset.sourceOrganizationId,
        publicDataset.sourceDatasetId,
        PublishStatus.PublishFailed,
        publicDatasetV1.version,
        Some("Error, error!")
      )

      processNotification(publishNotification) shouldBe an[MessageAction.Delete]

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
        )
        .awaitFinite()

      publicVersion.status shouldBe PublishStatus.PublishFailed

      ports.db
        .run(
          PublicFilesMapper
            .forVersion(publicVersion)
            .result
        )
        .awaitFinite() shouldBe empty

      ports.pennsieveApiClient
        .asInstanceOf[MockPennsieveApiClient]
        .publishCompleteRequests shouldBe List(
        (
          DatasetPublishStatus(
            publicDataset.name,
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId,
            None,
            0,
            PublishStatus.PublishFailed,
            None,
            workflowId = PublishingWorkflow.Version4
          ),
          Some(s"Version ${publicVersion.version} failed to publish")
        )
      )

      ports.doiClient
        .asInstanceOf[MockDoiClient]
        .dois(doi.doi)
        .state shouldBe Some(DoiState.Draft)

      val alert = ports.victorOpsClient
        .asInstanceOf[MockVictorOpsClient]
        .sentAlerts
        .head

      alert.version shouldBe publicVersion.version
      alert.publicDatasetId shouldBe publicVersion.datasetId
      alert.sourceOrganizationId shouldBe publishNotification.organizationId
      alert.sourceDatasetId shouldBe publishNotification.datasetId
    }

    "not index files and records for embargoed datasets" in {
      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.EmbargoInProgress,
        doi = doi.doi
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            totalSize = 76543
          )
        )

      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        )
      ) shouldBe an[MessageAction.Delete]

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
        )
        .awaitFinite()

      inside(publicVersion) {
        case v: PublicDatasetVersion =>
          v.status shouldBe PublishStatus.EmbargoSucceeded
      }

      val (
        indexedVersion,
        indexedRevision,
        indexedSponsorship,
        indexedFiles,
        indexedRecords
      ) =
        ports.searchClient
          .asInstanceOf[MockSearchClient]
          .indexedDatasets(publicDataset.id)

      indexedVersion.version shouldBe publicDatasetV1.version
      indexedRevision shouldBe None
      indexedSponsorship shouldBe None
      indexedFiles shouldBe empty
      indexedRecords shouldBe empty
    }

    "update publish status as failed for embargoed datasets" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.EmbargoInProgress,
        doi = doi.doi
      )

      val publishNotification = PublishNotification(
        publicDataset.sourceOrganizationId,
        publicDataset.sourceDatasetId,
        PublishStatus.PublishFailed,
        publicDatasetV1.version,
        Some("Error, error!")
      )

      processNotification(publishNotification) shouldBe an[MessageAction.Delete]

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
        )
        .awaitFinite()

      publicVersion.status shouldBe PublishStatus.EmbargoFailed

      ports.db
        .run(
          PublicFilesMapper
            .forVersion(publicVersion)
            .result
        )
        .awaitFinite() shouldBe empty

      ports.pennsieveApiClient
        .asInstanceOf[MockPennsieveApiClient]
        .publishCompleteRequests shouldBe List(
        (
          DatasetPublishStatus(
            publicDataset.name,
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId,
            None,
            0,
            PublishStatus.EmbargoFailed,
            None,
            workflowId = PublishingWorkflow.Version4
          ),
          Some(s"Version ${publicVersion.version} failed to publish")
        )
      )

      ports.doiClient
        .asInstanceOf[MockDoiClient]
        .dois(doi.doi)
        .state shouldBe Some(DoiState.Draft)

      val alert = ports.victorOpsClient
        .asInstanceOf[MockVictorOpsClient]
        .sentAlerts
        .head

      alert.version shouldBe publicVersion.version
      alert.publicDatasetId shouldBe publicVersion.datasetId
      alert.sourceOrganizationId shouldBe publishNotification.organizationId
      alert.sourceDatasetId shouldBe publishNotification.datasetId
    }

    "ignore messages that it cannot parse" in {

      val notificationMessage = Message
        .builder()
        .body("{\"field\" : \"not what I was expecting\"}")
        .build()

      val result =
        Source
          .single(notificationMessage)
          .via(notificationHandler.notificationFlow)
          .runWith(Sink.seq)
          .awaitFinite()
          .head

      result shouldBe an[MessageAction.Ignore]
    }

    "ignore messages that mention invalid versions" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.NotPublished
      )

      val notificationMessage = Message
        .builder()
        .body(
          PublishNotification(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId,
            PublishStatus.PublishSucceeded,
            12345 // bad version
          ).asJson.toString
        )
        .build()

      val result =
        Source
          .single(notificationMessage)
          .via(notificationHandler.notificationFlow)
          .runWith(Sink.seq)
          .awaitFinite()
          .head

      result shouldBe an[MessageAction.Ignore]

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
        )
        .awaitFinite()
        .status shouldBe PublishStatus.NotPublished
    }
  }

  "release an embargoed dataset" in {
    val datasetName = TestUtilities.randomString()

    val publicDataset =
      TestUtilities.createDataset(ports.db)(name = datasetName)

    val doi = ports.doiClient
      .asInstanceOf[MockDoiClient]
      .createMockDoi(
        publicDataset.sourceOrganizationId,
        publicDataset.sourceDatasetId
      )

    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishStatus.ReleaseInProgress,
      doi = doi.doi
    )

    TestUtilities.createFile(ports.db)(
      publicDatasetV1,
      "files/test.txt",
      "TEXT"
    )

    processNotification(
      ReleaseNotification(
        organizationId = publicDataset.sourceOrganizationId,
        datasetId = publicDataset.sourceDatasetId,
        version = publicDatasetV1.version,
        publishBucket = S3Bucket("publish-bucket"),
        embargoBucket = S3Bucket("embargo-bucket"),
        success = true
      )
    ) shouldBe an[MessageAction.Delete]

    val publicVersion = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
      )
      .awaitFinite()

    // Update S3 bucket to be public bucket, not embargo bucket
    publicVersion.s3Bucket shouldBe S3Bucket("publish-bucket")

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .publishCompleteRequests shouldBe List(
      (
        DatasetPublishStatus(
          publicDataset.name,
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          Some(publicDataset.id),
          1,
          PublishStatus.PublishSucceeded,
          Some(publicVersion.createdAt),
          workflowId = PublishingWorkflow.Version4
        ),
        None
      )
    )

    val (
      indexedVersion,
      indexedRevision,
      indexedSponsorship,
      indexedFiles,
      indexedRecords
    ) =
      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(publicDataset.id)

    indexedVersion.version shouldBe publicDatasetV1.version
    indexedRevision shouldBe None
    indexedSponsorship shouldBe None

    // From defaults in MockS3StreamClient
    indexedFiles.length shouldBe 1
    indexedRecords.length shouldBe 1
  }

  "update release status as failed" in {

    val datasetName = TestUtilities.randomString()

    val publicDataset =
      TestUtilities.createDataset(ports.db)(name = datasetName)

    val doi = ports.doiClient
      .asInstanceOf[MockDoiClient]
      .createMockDoi(
        publicDataset.sourceOrganizationId,
        publicDataset.sourceDatasetId
      )

    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishStatus.ReleaseInProgress,
      doi = doi.doi
    )

    TestUtilities.createFile(ports.db)(
      publicDatasetV1,
      "files/test.txt",
      "TEXT"
    )

    val releaseNotification = ReleaseNotification(
      organizationId = publicDataset.sourceOrganizationId,
      datasetId = publicDataset.sourceDatasetId,
      version = publicDatasetV1.version,
      publishBucket = S3Bucket("publish-bucket"),
      embargoBucket = S3Bucket("embargo-bucket"),
      success = false,
      Some("Error, error!")
    )

    processNotification(releaseNotification) shouldBe an[MessageAction.Delete]

    val publicVersion = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getVersion(publicDatasetV1.datasetId, publicDatasetV1.version)
      )
      .awaitFinite()

    publicVersion.s3Bucket shouldBe S3Bucket("embargo-bucket")
    publicVersion.status shouldBe PublishStatus.ReleaseFailed

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .publishCompleteRequests shouldBe List(
      (
        DatasetPublishStatus(
          publicDataset.name,
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          Some(publicDataset.id),
          0,
          PublishStatus.ReleaseFailed,
          Some(publicVersion.createdAt),
          workflowId = PublishingWorkflow.Version4
        ),
        Some(s"Version ${publicVersion.version} failed to release")
      )
    )

    val alert = ports.victorOpsClient
      .asInstanceOf[MockVictorOpsClient]
      .sentAlerts
      .head

    alert.version shouldBe publicVersion.version
    alert.publicDatasetId shouldBe publicVersion.datasetId
    alert.sourceOrganizationId shouldBe releaseNotification.organizationId
    alert.sourceDatasetId shouldBe releaseNotification.datasetId
  }

  "periodically notify Pennsieve API to start release workflow" in {
    val version1 =
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 3,
        status = PublishStatus.EmbargoSucceeded,
        embargoReleaseDate = Some(LocalDate.now)
      )

    val version2 =
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 5,
        status = PublishStatus.PublishSucceeded,
        embargoReleaseDate = Some(LocalDate.now)
      )

    processNotification(ScanForReleaseNotification()) shouldBe an[
      MessageAction.Delete
    ]

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .startReleaseRequests shouldBe List((1, 3))
  }

  "start release workflow for datasets with release dates in the past" in {
    TestUtilities.createDatasetV1(ports.db)(
      sourceOrganizationId = 1,
      sourceDatasetId = 3,
      status = PublishStatus.EmbargoSucceeded,
      embargoReleaseDate = Some(LocalDate.now.minusWeeks(1))
    )

    processNotification(ScanForReleaseNotification()) shouldBe an[
      MessageAction.Delete
    ]

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .startReleaseRequests shouldBe List((1, 3))
  }

  "do not start release workflow for datasets with release dates in the future" in {
    TestUtilities.createDatasetV1(ports.db)(
      status = PublishStatus.EmbargoSucceeded,
      embargoReleaseDate = Some(LocalDate.now.plusDays(1))
    )

    processNotification(ScanForReleaseNotification()) shouldBe an[
      MessageAction.Delete
    ]

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .startReleaseRequests shouldBe empty
  }

  "send VictorOps alert when unable to periodically start release workflow" in {
    val version1 =
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 999, // Magic number to trigger error in API client
        status = PublishStatus.EmbargoSucceeded,
        embargoReleaseDate = Some(LocalDate.now)
      )

    processNotification(ScanForReleaseNotification()) shouldBe an[
      MessageAction.Ignore
    ]

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .startReleaseRequests shouldBe List((1, 999))

    val alert = ports.victorOpsClient
      .asInstanceOf[MockVictorOpsClient]
      .sentAlerts
      .head

    alert.version shouldBe version1.version
    alert.publicDatasetId shouldBe version1.datasetId
    alert.sourceOrganizationId shouldBe 1
    alert.sourceDatasetId shouldBe 999
  }
}
