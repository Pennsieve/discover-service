// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import akka.stream.alpakka.sqs.MessageAction
import com.pennsieve.discover.ServiceSpecHarness
import com.pennsieve.models.{
  DatasetMetadataV4_0,
  DatasetMetadataV5_0,
  DatasetType,
  Degree,
  Doi,
  FileManifest,
  FileType,
  License,
  PublishStatus,
  PublishedContributor,
  PublishedExternalPublication,
  ReferenceMetadataV5_0,
  ReleaseMetadataV5_0
}
import com.pennsieve.models.DatasetMetadata._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.clients.{
  MockDoiClient,
  MockPennsieveApiClient,
  MockS3StreamClient,
  MockSearchClient
}
import com.pennsieve.discover.models.{
  PublicDataset,
  PublicDatasetVersion,
  PublicFile,
  PublicFileVersion,
  PublishJobOutput,
  PublishingWorkflow,
  ReleaseActionV50,
  S3Bucket,
  S3Key,
  WorkspaceSettings
}
import com.pennsieve.discover.db.{
  PublicDatasetVersionsMapper,
  PublicFileVersionsMapper,
  PublicFilesMapper,
  WorkspaceSettingsMapper
}
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.discover.server.definitions.DatasetPublishStatus
import com.pennsieve.discover.TestUtilities
import com.pennsieve.discover.notifications.SQSNotificationType.INDEX
import com.pennsieve.doi.client.definitions._
import com.pennsieve.models.RelationshipType.{ IsCitedBy, References }
import com.sksamuel.elastic4s.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import software.amazon.awssdk.services.sqs.model.{
  Message,
  ReceiveMessageRequest
}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate
import java.util.{ Calendar, UUID }
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SQSNotificationHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ServiceSpecHarness {

  override implicit val system: ActorSystem = ActorSystem("discover-service")

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  lazy val notificationHandler = new SQSNotificationHandler(
    ports,
    config.sqs.region,
    config.sqs.queueUrl,
    config.sqs.parallelism
  )

  /**
    * Run an incoming job done notification through the SQS notification stream.
    */
  def processNotification(
    notification: SQSNotification,
    waitTime: FiniteDuration = 10.seconds
  ): MessageAction = {
    val sqsMessage = Message
      .builder()
      .body(notification.asJson.toString)
      .build()

    Source
      .single(sqsMessage)
      .via(notificationHandler.notificationFlow)
      .runWith(Sink.seq)
      .awaitFinite(waitTime)
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
        embargoReleaseDate = Some(LocalDate.of(futureYear, 3, 14)),
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
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
          v.fileCount shouldBe 2
          v.readme shouldBe Some(v.s3Key / "readme.md")
          v.banner shouldBe Some(v.s3Key / "banner.jpg")
      }

      val files = ports.db
        .run(
          PublicFileVersionsMapper
            .forVersion(publicVersion)
            .result
        )
        .awaitFinite()

      // Should create database entries for newly published files
      files.length shouldBe 2

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
            workflowId = PublishingWorkflow.Version5
          ),
          None
        )
      )

      // pushing the DOI is now done asynchronously via separate SQS Message request
      //      val actualDoi: DoiDTO = ports.doiClient
      //        .asInstanceOf[MockDoiClient]
      //        .dois(doi.doi)
      //      actualDoi.title shouldBe Some(publicDataset.name)
      //      actualDoi.creators shouldBe Some(List())
      //      actualDoi.publicationYear shouldBe Some(futureYear)
      //      actualDoi.url shouldBe Some(
      //        s"https://discover.pennsieve.org/datasets/${publicDataset.id}/version/${publicDatasetV1.version}"
      //      )

      // indexing the published dataset is now done asynchronously via separate SQS Message request
      //      val (
      //        indexedVersion,
      //        indexedRevision,
      //        indexedSponsorship,
      //        indexedFiles,
      //        indexedRecords
      //      ) =
      //        ports.searchClient
      //          .asInstanceOf[MockSearchClient]
      //          .indexedDatasets(publicDataset.id)
      //
      //      indexedVersion.version shouldBe publicDatasetV1.version
      //      indexedRevision shouldBe None
      //      indexedSponsorship shouldBe None
      //
      //      // From defaults in MockS3StreamClient
      //      indexedFiles.length shouldBe 2
      //      indexedRecords.length shouldBe 1
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

    "update S3 Version Id and SHA256 when release completes" in {
      val sourceOrganizationId = 1
      val sourceDatasetId = 15

      // create a dataset
      val publicDataset =
        TestUtilities.createDataset(ports.db)(
          sourceOrganizationId = sourceOrganizationId,
          sourceDatasetId = sourceDatasetId
        )

      // create a dataset version, status = EmbargoSucceeded
      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishStatus.EmbargoSucceeded,
          doi = doi.doi,
          migrated = true
        )

      // create a file with path = {datasetId}/files/data/source.dat, S3 Version Id = x1, SHA256 = y1
      val path = "files/data/source.dat"
      val s3Key = s"${publicDataset.id}/${path}"
      val publicFileVersionEmbargoed =
        TestUtilities.createFileWithSha256(ports.db)(
          publicDatasetVersion,
          path,
          FileType.GenericData.toString,
          12345,
          None,
          Some("x1"),
          "y1"
        )

      // upload release results to Mock S3 Stream Client -
      //   path = {datasetId}/files/data/source.dat, S3 Version Id = x2, SHA256 = y2
      val results = List(
        ReleaseActionV50(
          sourceBucket = "embargo-bucket",
          sourceKey = s3Key,
          sourceSize = "12345",
          sourceVersionId = "x1",
          sourceEtag = "none",
          sourceSha256 = "y1",
          targetBucket = "bucket",
          targetKey = s3Key,
          targetSize = "12345",
          targetVersionId = "x2",
          targetEtag = "none",
          targetSha256 = "y2"
        )
      )
      val _ = ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .storeReleaseResults(publicDatasetVersion, results)
        .await

      // processNotification(...)
      processNotification(
        ReleaseNotification(
          organizationId = sourceOrganizationId,
          datasetId = sourceDatasetId,
          version = publicDatasetVersion.version,
          publishBucket = S3Bucket("bucket"),
          embargoBucket = S3Bucket("embargo-bucket"),
          success = true,
          error = None
        )
      ) shouldBe an[MessageAction.Delete]

      // get PublicFileVersion
      val publicFileVersionReleased = ports.db
        .run(
          PublicFileVersionsMapper
            .getFileVersion(publicDataset.id, S3Key.File(s3Key), "x2")
        )
        .await

      // check S3 Version Id and SHA256
      publicFileVersionReleased.s3Version shouldBe ("x2")
      publicFileVersionReleased.sha256.get shouldBe ("y2")
    }

    "handle multiple external publications with the same DOI and different relationships" in {

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
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      val externalDoi = TestUtilities.randomString()

      val externalPublications = List(
        new PublishedExternalPublication(
          doi = Doi(externalDoi),
          relationshipType = Some(References)
        ),
        new PublishedExternalPublication(
          doi = Doi(externalDoi),
          relationshipType = Some(IsCitedBy)
        )
      )

      for (externalPublication <- externalPublications) {
        TestUtilities.createExternalPublication(ports.db)(
          datasetId = publicDataset.id,
          version = publicDatasetV1.version,
          relationshipType = externalPublication.relationshipType.value,
          doi = externalPublication.doi.toString()
        )
      }

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate dataset V1 metadata (manifest.json)
      val metadataV1 = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = Some(externalPublications),
        files = TestUtilities.assetFiles(),
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV1.s3Key, metadataV1)

      // finalize publishing
      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      // push DOI to registry
      processNotification(
        PushDoiRequest(
          jobType = SQSNotificationType.PUSH_DOI,
          datasetId = publicDataset.id,
          version = publicDatasetV1.version,
          doi = publicDatasetV1.doi
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      val publishRequest = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .publishRequests
        .get(publicDatasetV1.doi)
        .value

      publishRequest.externalPublications.map(p => (p.doi, p.relationshipType)) should contain theSameElementsAs
        externalPublications
          .map(
            p => (p.doi.toString(), p.relationshipType.getOrElse(References))
          )
    }
  }

  "Discover Service SQS Queue Handler" should {
    "index a published dataset" in {
      val datasetVersion = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 3,
        status = PublishStatus.PublishSucceeded
      )

      processNotification(
        IndexDatasetRequest(
          jobType = INDEX,
          datasetId = datasetVersion.datasetId,
          version = datasetVersion.version
        )
      ) shouldBe an[MessageAction.Delete]

    }

    "handle large number of files in initial publication" in {
      val numberOfFiles = 10000

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
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate dataset metadata (manifest.json)
      val metadata = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ (1 to numberOfFiles).map { i =>
          val name = s"test-file-${i}.csv"
          FileManifest(
            name = name,
            path = s"data/${name}",
            size = TestUtilities.randomInteger(16 * 1024),
            fileType = FileType.CSV,
            sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
            id = None,
            s3VersionId = Some(TestUtilities.randomString()),
            sha256 = Some(TestUtilities.randomString())
          )
        }.toList,
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV1.s3Key, metadata)

      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

    }

    "handle large number of files in subsequent publication" in {
      val numberOfFilesV1 = 10000
      val numberOfFilesV2 = 10000

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
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate list of files in V1 of dataset
      val v1Files = (1 to numberOfFilesV1).map { i =>
        val name = s"test-file-${i}.csv"
        FileManifest(
          name = name,
          path = s"data/${name}",
          size = TestUtilities.randomInteger(16 * 1024),
          fileType = FileType.CSV,
          sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
          id = None,
          s3VersionId = Some(TestUtilities.randomString()),
          sha256 = Some(TestUtilities.randomString())
        )
      }.toList

      // generate dataset V1 metadata (manifest.json)
      val metadataV1 = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ v1Files,
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV1.s3Key, metadataV1)

      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      // create version 2
      val doiV2 = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.PublishInProgress,
        doi = doiV2.doi,
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV2.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      // generate list of files in V2 of dataset
      val v2Files = (1 to numberOfFilesV2).map { i =>
        val name = s"test-file-${i}.csv"
        FileManifest(
          name = name,
          path = s"data/${name}",
          size = TestUtilities.randomInteger(16 * 1024),
          fileType = FileType.CSV,
          sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
          id = None,
          s3VersionId = Some(TestUtilities.randomString()),
          sha256 = Some(TestUtilities.randomString())
        )
      }.toList

      // generate dataset V2 metadata (manifest.json)
      val metadataV2 = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doiV2.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ v1Files ++ v2Files,
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV2.s3Key, metadataV2)

      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV2.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

    }
  }

  "Workspace Metadata" should {
    "provide default settings" in {
      val numberOfFilesV1 = 10

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
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate list of files in V1 of dataset
      val v1Files = (1 to numberOfFilesV1).map { i =>
        val name = s"test-file-${i}.csv"
        FileManifest(
          name = name,
          path = s"data/${name}",
          size = TestUtilities.randomInteger(16 * 1024),
          fileType = FileType.CSV,
          sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
          id = None,
          s3VersionId = Some(TestUtilities.randomString()),
          sha256 = Some(TestUtilities.randomString())
        )
      }.toList

      // generate dataset V1 metadata (manifest.json)
      val metadataV1 = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ v1Files,
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV1.s3Key, metadataV1)

      // finalize publishing
      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      // push DOI to registry
      processNotification(
        PushDoiRequest(
          jobType = SQSNotificationType.PUSH_DOI,
          datasetId = publicDataset.id,
          version = publicDatasetV1.version,
          doi = publicDatasetV1.doi
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      val updatedDoiOption = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      updatedDoiOption should not be empty
      val updatedDoi = updatedDoiOption.get
      updatedDoi.publisher shouldBe WorkspaceSettings.defaultPublisher
      updatedDoi.url.get should startWith("https://discover.pennsieve")
    }

    "provide organization settings" in {
      val numberOfFilesV1 = 10
      val organizationId = 367
      val organizationName = "SPARC"
      val organizationRedirectUrlFormat =
        "https://sparc.science/datasets/{{datasetId}}/version/{{versionId}}"

      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat
      )

      ports.db.run(WorkspaceSettingsMapper.addSettings(settings)).await

      val publicDataset =
        TestUtilities.createDataset(ports.db)(
          sourceOrganizationId = organizationId,
          sourceOrganizationName = organizationName
        )

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      // Successful publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetV1.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetV1.s3Key / "readme.md",
            bannerKey = publicDatasetV1.s3Key / "banner.jpg",
            changelogKey = publicDatasetV1.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate list of files in V1 of dataset
      val v1Files = (1 to numberOfFilesV1).map { i =>
        val name = s"test-file-${i}.csv"
        FileManifest(
          name = name,
          path = s"data/${name}",
          size = TestUtilities.randomInteger(16 * 1024),
          fileType = FileType.CSV,
          sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
          id = None,
          s3VersionId = Some(TestUtilities.randomString()),
          sha256 = Some(TestUtilities.randomString())
        )
      }.toList

      // generate dataset V1 metadata (manifest.json)
      val metadataV1 = DatasetMetadataV4_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetV1.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetV1.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "dataset",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ v1Files,
        pennsieveSchemaVersion = "4.0"
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetV1.s3Key, metadataV1)

      // finalize publishing
      processNotification(
        PublishNotification(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          publicDatasetV1.version
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      // push DOI to registry
      processNotification(
        PushDoiRequest(
          jobType = SQSNotificationType.PUSH_DOI,
          datasetId = publicDataset.id,
          version = publicDatasetV1.version,
          doi = publicDatasetV1.doi
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      val updatedDoiOption = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      updatedDoiOption should not be empty
      val updatedDoi = updatedDoiOption.get
      updatedDoi.publisher shouldBe organizationName
      val expectedUrl =
        s"https://sparc.science/datasets/${publicDataset.id}/version/${publicDatasetV1.version}"
      updatedDoi.url.get shouldEqual (expectedUrl)
    }

    "provide a default release redirect url" in {
      val numberOfFilesV1 = 10

      val publicDataset =
        TestUtilities.createDataset(ports.db)(datasetType = DatasetType.Release)

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishStatus.PublishInProgress,
          doi = doi.doi,
          migrated = true
        )

      // Successfully publish jobs create an outputs.json file
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishResult(
          publicDatasetVersion.s3Key,
          PublishJobOutput(
            readmeKey = publicDatasetVersion.s3Key / "readme.md",
            bannerKey = publicDatasetVersion.s3Key / "banner.jpg",
            changelogKey = publicDatasetVersion.s3Key / "changelog.md",
            totalSize = 76543
          )
        )

      val datasetContributor = PublishedContributor(
        first_name = "dataset",
        last_name = "owner",
        orcid = Some("0000-0001-0023-9087"),
        middle_initial = None,
        degree = Some(Degree.PhD)
      )

      // generate list of files in V1 of dataset
      val v1Files = (1 to numberOfFilesV1).map { i =>
        val name = s"test-file-${i}.csv"
        FileManifest(
          name = name,
          path = s"data/${name}",
          size = TestUtilities.randomInteger(16 * 1024),
          fileType = FileType.CSV,
          sourcePackageId = Some(s"N:package:${UUID.randomUUID().toString}"),
          id = None,
          s3VersionId = Some(TestUtilities.randomString()),
          sha256 = Some(TestUtilities.randomString())
        )
      }.toList

      // generate dataset V1 metadata (manifest.json)
      val metadataV1 = DatasetMetadataV5_0(
        pennsieveDatasetId = publicDataset.id,
        version = publicDatasetVersion.version,
        revision = None,
        name = publicDataset.name,
        description = publicDatasetVersion.description,
        creator = datasetContributor,
        contributors = List(datasetContributor),
        sourceOrganization = "1",
        keywords = List("data"),
        datePublished = LocalDate.now(),
        license = Some(License.`Community Data License Agreement – Permissive`),
        `@id` = doi.doi,
        publisher = "Pennsieve",
        `@context` = "public data",
        `@type` = "release",
        schemaVersion = "n/a",
        collections = None,
        relatedPublications = None,
        files = TestUtilities.assetFiles() ++ v1Files,
        pennsieveSchemaVersion = "5.0",
        release = Some(
          ReleaseMetadataV5_0(
            origin = "GitHub",
            url = "https://github.com/some-org/some-repo",
            label = "v1.0.0",
            marker = "35fdff06784e4f28910d70ff0b9222b1"
          )
        )
      )

      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .withNextPublishMetadata(publicDatasetVersion.s3Key, metadataV1)

      // push DOI to registry
      processNotification(
        PushDoiRequest(
          jobType = SQSNotificationType.PUSH_DOI,
          datasetId = publicDataset.id,
          version = publicDatasetVersion.version,
          doi = publicDatasetVersion.doi
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      val updatedDoiOption = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      updatedDoiOption should not be empty
      val updatedDoi = updatedDoiOption.get
      updatedDoi.publisher shouldBe WorkspaceSettings.defaultPublisher
      updatedDoi.url.get should startWith("https://discover.pennsieve")
      updatedDoi.url.get should endWith(
        f"code/${publicDataset.id}/version/${publicDatasetVersion.version}"
      )
    }

    "provide tagged organization settings" in {
      val organizationId = ports.config.doiCollections.idSpace.id
      val publisherName = "SPARC Collections"
      val organizationRedirectUrlFormat =
        "https://sparc.science/collections/{{datasetId}}/version/{{versionId}}"
      val publishingTag = s"${PublicDataset.publisherTagKey}:sparc"

      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = publisherName,
        redirectUrl = organizationRedirectUrlFormat,
        publisherTag = Some(publishingTag)
      )

      ports.db.run(WorkspaceSettingsMapper.addSettings(settings)).await

      val tags = List("data", publishingTag)
      val publicDataset =
        TestUtilities.createDoiCollectionDataset(
          ports.db,
          ports.config.doiCollections.idSpace
        )(tags = tags)

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.PublishInProgress,
        doi = doi.doi,
        migrated = true
      )

      TestUtilities.createDatasetDoiCollection(ports.db)(
        datasetId = publicDatasetV1.datasetId,
        datasetVersion = publicDatasetV1.version,
        banners = List.empty
      )

      // push DOI to registry
      processNotification(
        PushDoiRequest(
          jobType = SQSNotificationType.PUSH_DOI,
          datasetId = publicDataset.id,
          version = publicDatasetV1.version,
          doi = publicDatasetV1.doi
        ),
        waitTime = 60.seconds
      ) shouldBe an[MessageAction.Delete]

      val updatedDoiOption = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId
        )

      updatedDoiOption should not be empty
      val updatedDoi = updatedDoiOption.get
      updatedDoi.publisher shouldBe publisherName
      val expectedUrl =
        s"https://sparc.science/collections/${publicDataset.id}/version/${publicDatasetV1.version}"
      updatedDoi.url.get shouldEqual (expectedUrl)
    }

  }
}
