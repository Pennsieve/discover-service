// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.{
  generateServiceToken,
  generateUserToken
}
import com.pennsieve.discover._
import com.pennsieve.discover.client.definitions
import com.pennsieve.discover.client.definitions.{
  DatasetPublishStatus,
  InternalCollection,
  InternalContributor,
  SponsorshipRequest,
  SponsorshipResponse
}
import com.pennsieve.discover.client.publish._
import com.pennsieve.discover.clients._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db._
import com.pennsieve.discover.models._
import com.pennsieve.discover.notifications.{
  PublishNotification,
  SQSNotificationHandler
}
import com.pennsieve.doi.models.DoiState
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.pennsieve.models.RelationshipType.{ IsCitedBy, References }
import com.pennsieve.models.{ Degree, License, PublishStatus, RelationshipType }
import com.pennsieve.test.EitherValue._
import io.circe.syntax._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.services.sqs.model.Message

import java.time.LocalDate
import scala.concurrent.duration._

class PublishHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(PublishHandler.routes(ports))

  def createClient(routes: Route): PublishClient =
    PublishClient.httpClient(Route.toFunction(routes))

  val organizationId = 1
  val organizationNodeId = "N:organization:abc123"
  val organizationName = "University of Pennsylvania"
  val datasetName = "Dataset"
  val datasetId = 34
  val datasetNodeId = "N:dataset:abc123"
  val ownerId = 1
  val ownerNodeId = "N:user:abc123"
  val ownerFirstName = "Data"
  val ownerLastName = "Digger"
  val ownerOrcid = "0000-0012-3456-7890"

  val token: Jwt.Token =
    generateServiceToken(
      ports.jwt,
      organizationId = organizationId,
      datasetId = datasetId
    )

  val userToken: Jwt.Token =
    generateUserToken(ports.jwt, 1, organizationId, Some(datasetId))

  val internalContributor =
    new InternalContributor(
      1,
      "Sally",
      "Field",
      middleInitial = Some("M"),
      degree = Some(Degree.BS)
    )

  val publicContributor =
    new PublicContributor(
      firstName = "Alfred",
      middleInitial = Some("C"),
      lastName = "Kinsey",
      degree = Some(Degree.PhD),
      orcid = None,
      datasetId = datasetId,
      versionId = 1,
      sourceContributorId = 1,
      sourceUserId = None
    )

  val internalExternalPublication =
    new definitions.InternalExternalPublication(
      doi = "10.26275/t6j6-77pu",
      relationshipType = Some(RelationshipType.Describes)
    )

  val customBucketConfig =
    definitions.BucketConfig("org-publish-bucket", "org-embargo-bucket")

  def customBucketReleaseBody(customBucketConfig: definitions.BucketConfig) =
    definitions.ReleaseRequest(Some(customBucketConfig))

  val defaultBucketReleaseBody: definitions.ReleaseRequest =
    definitions.ReleaseRequest()

  val defaultBucketUnpublishBody: definitions.UnpublishRequest =
    definitions.UnpublishRequest()

  val requestBody: definitions.PublishRequest = definitions.PublishRequest(
    name = datasetName,
    description = "A very very long description...",
    ownerId = 1,
    modelCount = Vector(definitions.ModelCount("myConcept", 100L)),
    recordCount = 100L,
    fileCount = 100L,
    size = 5555555L,
    license = License.`Apache 2.0`,
    contributors = Vector(internalContributor),
    externalPublications = Some(Vector(internalExternalPublication)),
    tags = Vector[String]("tag1", "tag2"),
    ownerNodeId = ownerNodeId,
    ownerFirstName = ownerFirstName,
    ownerLastName = ownerLastName,
    ownerOrcid = ownerOrcid,
    organizationNodeId = organizationNodeId,
    organizationName = organizationName,
    datasetNodeId = datasetNodeId,
    workflowId = Some(4)
  )

  val customBucketRequestBody: definitions.PublishRequest =
    requestBody.copy(bucketConfig = Some(customBucketConfig))

  val reviseRequest = definitions.ReviseRequest(
    name = "A different name",
    description = "Brief and succint.",
    license = License.MIT,
    contributors = Vector(
      new InternalContributor(
        1,
        "Sally",
        "Field",
        middleInitial = Some("M"),
        orcid = Some("1234-440304")
      ),
      new InternalContributor(
        781,
        "Alan",
        "Turing",
        middleInitial = Some("M"),
        degree = Some(Degree.PhD),
        userId = Some(8849)
      ),
      new InternalContributor(782, "Ada", "Lovelace")
    ),
    externalPublications = Some(
      Vector(
        internalExternalPublication,
        new definitions.InternalExternalPublication(
          "10.26275/v62f-qd4v",
          Some(RelationshipType.IsReferencedBy)
        )
      )
    ),
    tags = Vector[String]("red", "green", "blue"),
    ownerId = 99999,
    ownerFirstName = "Haskell",
    ownerLastName = "Curry",
    ownerOrcid = "0000-1111-1111-1111",
    bannerPresignedUrl =
      "https://s3.amazonaws.localhost/dev-dataset-assets-use1/banner.jpg",
    readmePresignedUrl =
      "https://s3.amazonaws.localhost/dev-dataset-assets-use1/readme.md",
    changelogPresignedUrl =
      "https://s3.amazonaws.localhost/dev-dataset-assets-use1/changelog.md",
    collections = Some(
      Vector[InternalCollection](
        new InternalCollection(id = 1, name = "An Amazing Collection")
      )
    )
  )

  val authToken = List(Authorization(OAuth2BearerToken(token.value)))

  val userAuthToken = List(Authorization(OAuth2BearerToken(userToken.value)))

  val client = createClient(createRoutes())

  lazy val notificationHandler = new SQSNotificationHandler(
    ports,
    config.sqs.region,
    config.sqs.queueUrl,
    config.sqs.parallelism
  )

  /**
    * Complete the publish job successfully
    */
  def publishSuccessfully(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  ): Unit = {

    // Successful publish jobs create an outputs.json file
    ports.s3StreamClient
      .asInstanceOf[MockS3StreamClient]
      .withNextPublishResult(
        version.s3Key,
        PublishJobOutput(
          readmeKey = version.s3Key / "readme.md",
          bannerKey = version.s3Key / "banner.jpg",
          changelogKey = version.s3Key / "changelog.md",
          totalSize = 76543
        )
      )

    val notificationMessage = Message
      .builder()
      .body(
        PublishNotification(
          dataset.sourceOrganizationId,
          dataset.sourceDatasetId,
          PublishStatus.PublishSucceeded,
          version.version
        ).asJson.toString
      )
      .build()

    val result =
      Source
        .single(notificationMessage)
        .via(notificationHandler.notificationFlow)
        .runWith(Sink.seq)
        .awaitFinite(10.seconds)
        .head

    result shouldBe an[MessageAction.Delete]
  }

  def run[A](
    dbio: DBIOAction[A, NoStream, Nothing],
    timeout: FiniteDuration = 5.seconds
  ) =
    ports.db.run(dbio).awaitFinite(timeout)

  "POST /organizations/{organizationId}/datasets/{datasetId}/publish" should {

    "fail without a JWT" in {

      val response = client
        .publish(organizationId, datasetId, None, None, requestBody)
        .awaitFinite()
        .value

      response shouldBe PublishResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .publish(
          organizationId,
          datasetId,
          None,
          None,
          requestBody,
          userAuthToken
        )
        .awaitFinite()
        .value

      response shouldBe PublishResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "create a DB entry, link a DOI, and launch the publish job" in {

      val response = client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe organizationId
      publicDataset.sourceDatasetId shouldBe datasetId
      publicDataset.ownerId shouldBe requestBody.ownerId
      publicDataset.ownerFirstName shouldBe requestBody.ownerFirstName
      publicDataset.ownerLastName shouldBe requestBody.ownerLastName
      publicDataset.ownerOrcid shouldBe requestBody.ownerOrcid

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion.version shouldBe 1
      publicVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      publicVersion.recordCount shouldBe requestBody.recordCount
      publicVersion.fileCount shouldBe requestBody.fileCount
      publicVersion.size shouldBe requestBody.size
      publicVersion.description shouldBe requestBody.description
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe config.s3.publishBucket
      publicVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${publicVersion.version}/"
      )
      publicVersion.doi shouldBe doiDto.doi

      val externalPublications = run(
        PublicExternalPublicationsMapper
          .getByDatasetAndVersion(publicDataset, publicVersion)
      )
      externalPublications.map(_.doi) shouldBe List(
        internalExternalPublication.doi
      )
      externalPublications.map(_.relationshipType) shouldBe List(
        RelationshipType.Describes
      )
      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      inside(publishedJobs.toList) {
        case job :: Nil =>
          inside(job.contributors) {
            case contributor :: Nil =>
              contributor.datasetId shouldBe publicDataset.id
              contributor.versionId shouldBe publicVersion.version
              contributor.firstName shouldBe "Sally"
              contributor.lastName shouldBe "Field"
              contributor.middleInitial shouldBe Some("M")
              contributor.degree shouldBe Some(Degree.BS)
              contributor.orcid shouldBe None
              contributor.sourceContributorId shouldBe 1
              contributor.sourceUserId shouldBe None
          }

          inside(job.externalPublications) {
            case externalPublication :: Nil =>
              externalPublication should matchPattern {
                case PublicExternalPublication(
                    "10.26275/t6j6-77pu",
                    RelationshipType.Describes,
                    _,
                    _,
                    _,
                    _
                    ) =>
              }
          }

          job.organizationId shouldBe organizationId
          job.organizationNodeId shouldBe organizationNodeId
          job.organizationName shouldBe organizationName
          job.datasetId shouldBe datasetId
          job.datasetNodeId shouldBe datasetNodeId
          job.publishedDatasetId shouldBe publicDataset.id
          job.userId shouldBe requestBody.ownerId
          job.userNodeId shouldBe requestBody.ownerNodeId
          job.userFirstName shouldBe requestBody.ownerFirstName
          job.userLastName shouldBe requestBody.ownerLastName
          job.userOrcid shouldBe requestBody.ownerOrcid
          job.s3Bucket shouldBe config.s3.publishBucket
          job.s3PgdumpKey shouldBe (
            S3Key
              .Version(publicDataset.id, publicVersion.version) / "dump.sql"
          )
          job.s3PublishKey shouldBe S3Key.Version(
            publicDataset.id,
            publicVersion.version
          )
          job.version shouldBe publicVersion.version
          job.doi shouldBe doiDto.doi

      }
    }

    "correctly use custom publish bucket" in {

      val response = client
        .publish(
          organizationId,
          datasetId,
          None,
          None,
          customBucketRequestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      publicVersion.s3Bucket.value shouldBe customBucketConfig.publish

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs should have length 1
      publishedJobs.head.s3Bucket.value shouldBe customBucketConfig.publish
    }

    "publish to an embargo bucket" in {
      val expectedEmbargoReleaseDate = LocalDate.now().plusDays(30)

      val response = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(expectedEmbargoReleaseDate),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishStatus.EmbargoInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe organizationId
      publicDataset.sourceDatasetId shouldBe datasetId
      publicDataset.ownerId shouldBe requestBody.ownerId
      publicDataset.ownerFirstName shouldBe requestBody.ownerFirstName
      publicDataset.ownerLastName shouldBe requestBody.ownerLastName
      publicDataset.ownerOrcid shouldBe requestBody.ownerOrcid

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion.version shouldBe 1
      publicVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      publicVersion.recordCount shouldBe requestBody.recordCount
      publicVersion.fileCount shouldBe requestBody.fileCount
      publicVersion.size shouldBe requestBody.size
      publicVersion.description shouldBe requestBody.description
      publicVersion.status shouldBe PublishStatus.EmbargoInProgress
      publicVersion.s3Bucket shouldBe config.s3.embargoBucket
      publicVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${publicVersion.version}/"
      )
      publicVersion.doi shouldBe doiDto.doi
      publicVersion.embargoReleaseDate shouldBe Some(expectedEmbargoReleaseDate)

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs.length shouldBe 1
      publishedJobs.head.s3Bucket shouldBe config.s3.embargoBucket
    }

    "correctly use custom embargo bucket" in {
      val expectedEmbargoReleaseDate = LocalDate.now().plusDays(30)

      val response = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(expectedEmbargoReleaseDate),
          customBucketRequestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      publicVersion.s3Bucket.value shouldBe customBucketConfig.embargo

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs.length shouldBe 1
      publishedJobs.head.s3Bucket.value shouldBe customBucketConfig.embargo
    }

    "return the publishing status of the dataset" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      val publicDataset1_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val response = client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        Some(publicDataset.id),
        1,
        PublishInProgress,
        Some(publicDataset1_V1.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion.version shouldBe 2
      publicVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      publicVersion.recordCount shouldBe requestBody.recordCount
      publicVersion.fileCount shouldBe requestBody.fileCount
      publicVersion.size shouldBe requestBody.size
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe S3Bucket("bucket")
      publicVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${publicVersion.version}/"
      )
      publicVersion.doi shouldBe doiDto.doi

    }

    "delete a previously failed version before creating a new one" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed
      )

      val response = client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      latestVersion.version shouldBe 1
      latestVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      latestVersion.recordCount shouldBe requestBody.recordCount
      latestVersion.fileCount shouldBe requestBody.fileCount
      latestVersion.size shouldBe requestBody.size
      latestVersion.status shouldBe PublishStatus.PublishInProgress
      latestVersion.s3Bucket shouldBe S3Bucket("bucket")
      latestVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${latestVersion.version}/"
      )
      latestVersion.doi shouldBe doiDto.doi

    }

    "delete an embargoed dataset version before publishing a new version" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishStatus.EmbargoSucceeded
      )

      val response = client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      latestVersion.version shouldBe 1
      latestVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      latestVersion.recordCount shouldBe requestBody.recordCount
      latestVersion.fileCount shouldBe requestBody.fileCount
      latestVersion.size shouldBe requestBody.size
      latestVersion.status shouldBe PublishStatus.PublishInProgress
      latestVersion.s3Bucket shouldBe S3Bucket("bucket")
      latestVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${latestVersion.version}/"
      )
      latestVersion.doi shouldBe doiDto.doi

    }

    "fail to embargo a dataset that is already published" in {

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishSucceeded
      )

      val response = client
        .publish(
          organizationId,
          datasetId,
          embargo = Some(true),
          embargoReleaseDate = Some(LocalDate.now()),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe a[PublishResponse.Forbidden]
    }

    "fail to embargo a dataset without a release date" in {
      val response = client
        .publish(
          organizationId,
          datasetId,
          embargo = Some(true),
          embargoReleaseDate = None,
          requestBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe a[PublishResponse.BadRequest]
    }

    "embargo a dataset after it has been unpublished" in {

      // Unpublished DOIs are set to Registered
      val unpublishedDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          organizationId,
          datasetId,
          state = Some(DoiState.Registered)
        )
        .doi
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = Unpublished,
        doi = unpublishedDoi
      )

      val response = client
        .publish(
          organizationId,
          datasetId,
          embargo = Some(true),
          embargoReleaseDate = Some(LocalDate.now()),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe a[PublishResponse.Created]
    }

    "publish a dataset after it has been unpublished" in {
      val v1Unpublished = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.Unpublished,
        migrated = true
      )

      v1Unpublished.version shouldBe 1

      // mock DOI client only needs to know the
      // most recent DOI.
      // Unpublished DOIs are set to Registered
      val unpublishedDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(
          organizationId,
          datasetId,
          state = Some(DoiState.Registered)
        )
        .doi

      val v2Unpublished = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.Unpublished,
        doi = unpublishedDoi,
        migrated = true
      )

      v2Unpublished.version shouldBe 2

      val response = client
        .publish(
          organizationId,
          datasetId,
          None,
          None,
          requestBody.copy(workflowId = Some(5)),
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe a[PublishResponse.Created]

      val republish = response.asInstanceOf[PublishResponse.Created].value

      republish.publishedDatasetId.value shouldBe v2Unpublished.datasetId
      republish.publishedVersionCount shouldBe 0
      republish.status shouldBe PublishInProgress

      val dataset =
        run(PublicDatasetsMapper.getDataset(republish.publishedDatasetId.value))

      val version = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(republish.publishedDatasetId.value)
      ).value

      publishSuccessfully(dataset, version)

      val latest = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(republish.publishedDatasetId.value)
      ).value

      latest.version shouldBe 3
      latest.status shouldBe PublishSucceeded

    }

    "not create a new DOI if draft DOI is only associated with a failed version" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      val draftDoi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .createMockDoi(organizationId, datasetId)
        .doi

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishFailed,
        doi = draftDoi
      )

      client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val latestVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      latestVersion.doi shouldBe draftDoi

    }

    "allow an external publication to be listed multiple times with different relationships" in {

      val internalExternalPubAgainWithDifferentType =
        internalExternalPublication.copy(relationshipType = Some(IsCitedBy))

      val internalExternalPubAgainWithDefaultType =
        internalExternalPublication.copy(relationshipType = None)

      val internalExternalPubs = Vector(
        internalExternalPublication,
        internalExternalPubAgainWithDifferentType,
        internalExternalPubAgainWithDefaultType
      )

      val testRequestBody =
        requestBody.copy(externalPublications = Some(internalExternalPubs))

      val response = client
        .publish(
          organizationId,
          datasetId,
          None,
          None,
          testRequestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val externalPublications = run(
        PublicExternalPublicationsMapper
          .getByDatasetAndVersion(publicDataset, publicVersion)
      )

      externalPublications should have length internalExternalPubs.length

      val externalPubData =
        externalPublications.map(p => (p.doi, p.relationshipType))

      val internalExternalPubData = internalExternalPubs.map(
        p => (p.doi, p.relationshipType.getOrElse(References))
      )

      externalPubData should contain theSameElementsAs internalExternalPubData

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs should have length 1

      publishedJobs.head.externalPublications should have length internalExternalPubs.length

      val jobExternalPubData = publishedJobs.head.externalPublications
        .map(p => (p.doi, p.relationshipType))

      jobExternalPubData should contain theSameElementsAs internalExternalPubData

    }

  }

  "POST /organizations/{organizationId}/datasets/{datasetId}/revise" should {

    "fail without a JWT" in {

      val response = client
        .revise(organizationId, datasetId, reviseRequest)
        .awaitFinite()
        .value

      response shouldBe ReviseResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .revise(organizationId, datasetId, reviseRequest, userAuthToken)
        .awaitFinite()
        .value

      response shouldBe ReviseResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "create a new dataset revision" in {
      client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      val version = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(publicDataset.id)
      ).get

      // Should have no revisions
      run(RevisionsMapper.getLatestRevision(version)) shouldBe None

      // "Complete" the publish job
      publishSuccessfully(publicDataset, version)

      val response =
        client
          .revise(organizationId, datasetId, reviseRequest, authToken)
          .awaitFinite()
          .value
          .asInstanceOf[ReviseResponse.Created]
          .value

      response shouldBe DatasetPublishStatus(
        "A different name",
        organizationId,
        datasetId,
        Some(publicDataset.id),
        1,
        PublishSucceeded,
        Some(version.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val revisedDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      val revisedVersion = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(publicDataset.id)
      ).get

      // Creates a new row in the revision table
      val revision = run(RevisionsMapper.getLatestRevision(version)).get
      revision.revision shouldBe 1

      // Updates dataset and version tables
      revisedDataset.name shouldBe "A different name"
      revisedDataset.license shouldBe License.MIT
      revisedDataset.tags shouldBe List("red", "green", "blue")
      revisedDataset.ownerId shouldBe 99999
      revisedDataset.ownerFirstName shouldBe "Haskell"
      revisedDataset.ownerLastName shouldBe "Curry"
      revisedDataset.ownerOrcid shouldBe "0000-1111-1111-1111"

      revisedVersion.status shouldBe PublishSucceeded
      revisedVersion.description shouldBe "Brief and succint."
      revisedVersion.size shouldBe (76543 + 400) // From publish notification + new files
      revisedVersion.banner shouldBe Some(
        revisedVersion.s3Key / s"revisions/${revision.revision}/banner.jpg"
      )
      revisedVersion.readme shouldBe Some(
        revisedVersion.s3Key / s"revisions/${revision.revision}/readme.md"
      )
      revisedVersion.changelog shouldBe Some(
        revisedVersion.s3Key / s"revisions/${revision.revision}/changelog.md"
      )

      // Updates contributors table
      val revisedContributors = run(
        PublicContributorsMapper
          .getContributorsByDatasetAndVersion(publicDataset, version)
      )

      revisedContributors.map(
        c =>
          (
            c.firstName,
            c.middleInitial,
            c.lastName,
            c.degree,
            c.orcid,
            c.sourceContributorId,
            c.sourceUserId
          )
      ) shouldBe List(
        ("Sally", Some("M"), "Field", None, Some("1234-440304"), 1, None),
        ("Alan", Some("M"), "Turing", Some(Degree.PhD), None, 781, Some(8849)),
        ("Ada", None, "Lovelace", None, None, 782, None)
      )

      // Updates external publications
      val externalPublications = run(
        PublicExternalPublicationsMapper
          .getByDatasetAndVersion(publicDataset, version)
      )
      externalPublications.map(_.doi) should contain theSameElementsAs List(
        internalExternalPublication.doi,
        "10.26275/v62f-qd4v"
      )

      // Updates DOI
      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      doi.title shouldBe Some("A different name")
      doi.creators shouldBe Some(
        List(Some("Sally M Field"), Some("Alan M Turing"), Some("Ada Lovelace"))
      )

      // Should write a manifest, readme and banner to `revisions/1/`
      inside(
        ports.s3StreamClient
          .asInstanceOf[MockS3StreamClient]
          .revisions
          .toList
      ) {
        case (d, v, c, r) :: Nil =>
          d.id shouldBe revisedDataset.id
          v.version shouldBe version.version
          c.map(_.id) shouldBe revisedContributors.map(_.id)
          r.revision shouldBe revision.revision
      }

      // And add those files to the Postgres files table
      val createdFiles = run(
        PublicFilesMapper
          .forVersion(revisedVersion)
          .filter(
            _.s3Key inSet List(
              revisedVersion.s3Key / s"revisions/${revision.revision}/manifest.json",
              revisedVersion.s3Key / s"revisions/${revision.revision}/readme.md",
              revisedVersion.s3Key / s"revisions/${revision.revision}/banner.jpg",
              revisedVersion.s3Key / s"revisions/${revision.revision}/changelog.md"
            )
          )
          .sortBy(_.name)
          .result
      )

      createdFiles.map(f => (f.name, f.fileType)) shouldBe List(
        ("banner.jpg", "JPEG"),
        ("changelog.md", "Markdown"),
        ("manifest.json", "Json"),
        ("readme.md", "Markdown")
      )

      // Reindexes the dataset with the revision
      val (indexedVersion, indexedRevision, _, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(revisedDataset.id)
      indexedVersion.version shouldBe revisedVersion.version
      indexedRevision.get.revision shouldBe revision.revision
    }

    "revise datasets with old schema versions" in {
      client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      val version = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(publicDataset.id)
      ).get

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === publicDataset.id)
          .map(v => (v.schemaVersion, v.status))
          .update(
            (PennsieveSchemaVersion.`3.0`, PublishStatus.PublishSucceeded)
          )
      )

      val response =
        client
          .revise(organizationId, datasetId, reviseRequest, authToken)
          .awaitFinite()
          .value
          .asInstanceOf[ReviseResponse.Created]
          .value

      response shouldBe DatasetPublishStatus(
        "A different name",
        organizationId,
        datasetId,
        Some(publicDataset.id),
        1,
        PublishSucceeded,
        Some(version.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val revision = run(RevisionsMapper.getLatestRevision(version)).get

      val revisedDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      val revisedVersion = run(
        PublicDatasetVersionsMapper
          .getLatestVersion(publicDataset.id)
      ).get

      // Reindexes the dataset with the revision
      val (indexedVersion, indexedRevision, _, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(revisedDataset.id)

      indexedVersion.version shouldBe revisedVersion.version
      indexedRevision.get.revision shouldBe revision.revision
    }

    "not revise a dataset that failed to publish" in {

      client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === publicDataset.id)
          .map(v => v.status)
          .update(PublishStatus.PublishFailed)
      )

      val response = client
        .revise(organizationId, datasetId, reviseRequest, authToken)
        .awaitFinite()
        .value

      response shouldBe a[ReviseResponse.Forbidden]
    }

    "not revise embargoed datasets" in {
      client
        .publish(
          organizationId,
          datasetId,
          embargo = Some(true),
          Some(LocalDate.now),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = run(
        PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === publicDataset.id)
          .map(v => v.status)
          .update(PublishStatus.EmbargoSucceeded)
      )

      val response = client
        .revise(organizationId, datasetId, reviseRequest, authToken)
        .awaitFinite()
        .value

      response shouldBe a[ReviseResponse.Forbidden]
    }
  }

  "POST /organizations/{organizationId}/datasets/{datasetId}/release" should {

    "fail without a JWT" in {

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody)
        .awaitFinite()
        .value

      response shouldBe ReleaseResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .release(
          organizationId,
          datasetId,
          defaultBucketReleaseBody,
          userAuthToken
        )
        .awaitFinite()
        .value

      response shouldBe ReleaseResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "fail to release a dataset that is not embargoed" in {
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.PublishSucceeded
      )

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Forbidden]
        .value
    }

    "fail to release a dataset that failed to embargo" in {
      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoFailed
      )

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Forbidden]
        .value
    }

    "fail to release a dataset that does not exist" in {

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody, authToken)
        .awaitFinite()
        .value shouldBe ReleaseResponse.NotFound
    }

    "release an embargoed dataset for an org with a custom bucket config" in {
      TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoSucceeded,
        s3Bucket = customBucketConfig.embargo
      )

      val response = client
        .release(
          organizationId,
          datasetId,
          customBucketReleaseBody(customBucketConfig),
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Accepted]
        .value

      val (publicDataset, version) = run(for {
        dataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)

        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
      } yield (dataset, version.get))

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        Some(publicDataset.id),
        0,
        PublishStatus.ReleaseInProgress,
        Some(version.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val releaseJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedReleaseJobs

      inside(releaseJobs.toList) {
        case job :: Nil =>
          job shouldBe EmbargoReleaseJob(
            organizationId = organizationId,
            datasetId = datasetId,
            version = version.version,
            s3Key = version.s3Key,
            publishBucket = S3Bucket(customBucketConfig.publish),
            embargoBucket = S3Bucket(customBucketConfig.embargo)
          )
      }
    }

    "use the default publish bucket when no publishBucket is provided in the ReleaseRequest" in {
      TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoSucceeded
      )

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Accepted]
        .value

      val (publicDataset, version) = run(for {
        dataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)

        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
      } yield (dataset, version.get))

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        Some(publicDataset.id),
        0,
        PublishStatus.ReleaseInProgress,
        Some(version.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val releaseJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedReleaseJobs

      inside(releaseJobs.toList) {
        case job :: Nil =>
          job shouldBe EmbargoReleaseJob(
            organizationId = organizationId,
            datasetId = datasetId,
            version = version.version,
            s3Key = version.s3Key,
            publishBucket = config.s3.publish50Bucket,
            embargoBucket = version.s3Bucket
          )
      }
    }

    "release a dataset for an org with a custom bucket config that was embargoed pre custom buckets" in {
      TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoSucceeded,
        s3Bucket = config.s3.embargoBucket.value
      )

      val response = client
        .release(
          organizationId,
          datasetId,
          customBucketReleaseBody(customBucketConfig),
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Accepted]
        .value

      val (publicDataset, version) = run(for {
        dataset <- PublicDatasetsMapper
          .getDatasetFromSourceIds(organizationId, datasetId)

        version <- PublicDatasetVersionsMapper
          .getLatestVersion(dataset.id)
      } yield (dataset, version.get))

      // Still the embargo bucket because this is not reset until
      // we are notified that step function completes successfully.
      version.s3Bucket.value shouldBe config.s3.embargoBucket.value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        Some(publicDataset.id),
        0,
        PublishStatus.ReleaseInProgress,
        Some(version.createdAt),
        workflowId = PublishingWorkflow.Version4
      )

      val releaseJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedReleaseJobs

      inside(releaseJobs.toList) {
        case job :: Nil =>
          job shouldBe EmbargoReleaseJob(
            organizationId = organizationId,
            datasetId = datasetId,
            version = version.version,
            s3Key = version.s3Key,
            publishBucket = S3Bucket(customBucketConfig.publish),
            embargoBucket = S3Bucket(config.s3.embargoBucket.value)
          )
      }
    }

    "restart a failed release job" in {
      TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.ReleaseFailed
      )

      val response = client
        .release(organizationId, datasetId, defaultBucketReleaseBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Accepted]
        .value

      response.status shouldBe PublishStatus.ReleaseInProgress
    }
  }

  "POST /organizations/{organizationId}/datasets/{datasetId}/unpublish" should {

    "fail without a JWT" in {

      val response = client
        .unpublish(organizationId, datasetId, defaultBucketUnpublishBody)
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          userAuthToken
        )
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Forbidden(
        "Only allowed for service level requests"
      )
    }

    "unpublish a dataset" in {

      val status = client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val version: PublicDatasetVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      // Complete publication process
      publishSuccessfully(publicDataset, version)

      val response =
        client
          .unpublish(
            organizationId,
            datasetId,
            defaultBucketUnpublishBody,
            authToken
          )
          .awaitFinite()
          .value
          .asInstanceOf[UnpublishResponse.OK]
          .value

      response.status shouldBe Unpublished

      // Note: the S3 Clean Lambda is invoked at the end of Publish to "tidy," and at Unpublish time
      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests should contain atLeastOneElementOf List(
        LambdaRequest(
          publicDataset.id.toString,
          publicDataset.id,
          None,
          version.s3Bucket.value,
          version.s3Bucket.value,
          S3CleanupStage.Unpublish,
          false
        )
      )

      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets shouldBe empty
    }

    "unpublish a dataset that has been published multiple times to different publish buckets" in {

      //Publish to default bucket
      client
        .publish(organizationId, datasetId, None, None, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val v1: PublicDatasetVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      // Complete publication process
      publishSuccessfully(publicDataset, v1)

      //Publish to custom bucket
      client
        .publish(
          organizationId,
          datasetId,
          None,
          None,
          requestBody.copy(bucketConfig = Some(customBucketConfig)),
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val v2: PublicDatasetVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      // Complete publication process
      publishSuccessfully(publicDataset, v2)

      val response =
        client
          .unpublish(
            organizationId,
            datasetId,
            defaultBucketUnpublishBody,
            authToken
          )
          .awaitFinite()
          .value
          .asInstanceOf[UnpublishResponse.OK]
          .value

      response.status shouldBe Unpublished

      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests should contain atLeastOneElementOf List(
        LambdaRequest(
          publicDataset.id.toString,
          publicDataset.id,
          None,
          v1.s3Bucket.value,
          v2.s3Bucket.value,
          S3CleanupStage.Unpublish,
          false
        )
      )

      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets shouldBe empty
    }

    "rollback a failed version when unpublishing" in {

      val version = TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.PublishFailed
      )

      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[UnpublishResponse.OK]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishStatus.NotPublished,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === version.datasetId)
          .result
      ) shouldBe empty

      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests shouldBe empty

    }

    "rollback an embargoed version when unpublishing" in {

      val status = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(LocalDate.now().plusDays(1)),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val version: PublicDatasetVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      publishSuccessfully(publicDataset, version)

      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[UnpublishResponse.OK]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishStatus.NotPublished,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === version.datasetId)
          .result
      ) shouldBe empty

      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .requests should contain atLeastOneElementOf List(
        LambdaRequest(
          publicDataset.id.toString,
          publicDataset.id,
          None,
          version.s3Bucket.value,
          version.s3Bucket.value,
          S3CleanupStage.Unpublish,
          false
        )
      )

    }

    "fail to unpublish a dataset that is currently publishing" in {

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.PublishInProgress
      )

      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Forbidden(
        "Cannot unpublish a dataset that is being published"
      )
    }

    "fail to unpublish a dataset that is being embargoed" in {

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoInProgress
      )

      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Forbidden(
        "Cannot unpublish a dataset that is being published"
      )
    }

    "fail to unpublish a dataset that is being released" in {

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.ReleaseInProgress
      )

      val response = client
        .unpublish(
          organizationId,
          datasetId,
          defaultBucketUnpublishBody,
          authToken
        )
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Forbidden(
        "Cannot unpublish a dataset that is being published"
      )
    }

    "respond with NoContent for a dataset that was never published" in {

      val response =
        client
          .unpublish(
            organizationId,
            datasetId,
            defaultBucketUnpublishBody,
            authToken
          )
          .awaitFinite()
          .value

      response shouldBe UnpublishResponse.NoContent
    }

  }

  "GET /organizations/{organizationId}/datasets/{datasetId}" should {

    "return the publishing status of the dataset" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val response =
        client
          .getStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId,
            authToken
          )
          .awaitFinite()
          .value

      val expected = DatasetPublishStatus(
        publicDataset.name,
        publicDataset.sourceOrganizationId,
        publicDataset.sourceDatasetId,
        Some(publicDataset.id),
        1,
        PublishSucceeded,
        Some(publicDatasetV1.createdAt),
        workflowId = PublishingWorkflow.Version4
      )
      response shouldBe GetStatusResponse.OK(expected)
    }
  }

  "GET /organizations/{organizationId}/datasets" should {

    "return the publishing status of all of the organization's datasets" in {

      val publicDataset1 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId
      )
      val publicDataset1_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishSucceeded
        )

      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = 35
      )
      val publicDataset2_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishSucceeded
        )

      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset2.id,
        status = PublishInProgress
      )

      ports.db
        .run(
          SponsorshipsMapper.createOrUpdate(
            publicDataset2.sourceOrganizationId,
            publicDataset2.sourceDatasetId,
            Some("foo"),
            Some("bar"),
            None
          )
        )
        .awaitFinite()

      val response =
        client
          .getStatuses(organizationId, authToken)
          .awaitFinite()
          .value

      val expected = Vector(
        DatasetPublishStatus(
          publicDataset1.name,
          publicDataset1.sourceOrganizationId,
          publicDataset1.sourceDatasetId,
          Some(publicDataset1.id),
          1,
          PublishSucceeded,
          Some(publicDataset1_V1.createdAt),
          workflowId = PublishingWorkflow.Version4
        ),
        DatasetPublishStatus(
          publicDataset2.name,
          publicDataset2.sourceOrganizationId,
          publicDataset2.sourceDatasetId,
          Some(publicDataset2.id),
          1,
          PublishInProgress,
          Some(publicDataset2_V1.createdAt),
          Some(SponsorshipRequest(Some("foo"), Some("bar"), None)),
          workflowId = PublishingWorkflow.Version4
        )
      )

      response shouldBe a[GetStatusesResponse.OK]
      response
        .asInstanceOf[GetStatusesResponse.OK]
        .value should contain theSameElementsAs expected
    }
  }

  "POST /organizations/{sourceOrganizationId}/datasets/{sourceDatasetId}/sponsor" should {

    "fail without a JWT" in {

      val response = client
        .sponsorDataset(
          organizationId,
          datasetId,
          SponsorshipRequest(Some("foo"), Some("bar"))
        )
        .awaitFinite()
        .value

      response shouldBe SponsorDatasetResponse.Unauthorized
    }

    "create and update sponsorships" in {

      val dataset =
        TestUtilities.createDataset(ports.db)(
          sourceOrganizationId = organizationId,
          sourceDatasetId = datasetId
        )

      TestUtilities.createNewDatasetVersion(ports.db)(
        dataset.id,
        status = PublishStatus.PublishSucceeded
      )

      val response = client
        .sponsorDataset(
          organizationId,
          datasetId,
          SponsorshipRequest(Some("foo"), Some("bar")),
          authToken
        )
        .awaitFinite()
        .value

      val savedSponsorship =
        ports.db
          .run(SponsorshipsMapper.getByDataset(dataset))
          .awaitFinite()

      response shouldBe SponsorDatasetResponse.Created(
        SponsorshipResponse(datasetId = dataset.id, savedSponsorship.id)
      )

      savedSponsorship shouldBe Sponsorship(
        dataset.id,
        Some("foo"),
        Some("bar"),
        None,
        savedSponsorship.id
      )

      val (_, _, indexedSponsorship, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(dataset.id)

      indexedSponsorship shouldBe Some(savedSponsorship)

      val updateResponse = client
        .sponsorDataset(
          organizationId,
          datasetId,
          SponsorshipRequest(Some("baz"), Some("qux"), Some("buzz")),
          authToken
        )
        .awaitFinite()
        .value

      updateResponse shouldBe SponsorDatasetResponse.Created(
        SponsorshipResponse(datasetId = dataset.id, 2)
      )

      val updatedSavedSponsorship =
        ports.db
          .run(SponsorshipsMapper.getByDataset(dataset))
          .awaitFinite()
      updatedSavedSponsorship shouldBe Sponsorship(
        dataset.id,
        Some("baz"),
        Some("qux"),
        Some("buzz"),
        2
      )

      val (_, _, indexedUpdatedSponsorship, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(dataset.id)

      indexedUpdatedSponsorship shouldBe Some(updatedSavedSponsorship)
    }
  }

  "DELETE /organizations/{sourceOrganizationId}/datasets/{sourceDatasetId}/sponsor" should {

    "fail without a JWT" in {

      val response = client
        .removeDatasetSponsor(organizationId, datasetId)
        .awaitFinite()
        .value

      response shouldBe RemoveDatasetSponsorResponse.Unauthorized
    }

    "remove sponsorships" in {

      val dataset =
        TestUtilities.createDataset(ports.db)(
          sourceOrganizationId = organizationId,
          sourceDatasetId = datasetId
        )

      TestUtilities.createNewDatasetVersion(ports.db)(
        dataset.id,
        status = PublishStatus.EmbargoSucceeded
      )

      val response = client
        .sponsorDataset(
          organizationId,
          datasetId,
          SponsorshipRequest(Some("foo"), Some("bar")),
          authToken
        )
        .awaitFinite()
        .value

      val savedSponsorship =
        ports.db
          .run(SponsorshipsMapper.getByDataset(dataset))
          .awaitFinite()

      response shouldBe SponsorDatasetResponse.Created(
        SponsorshipResponse(datasetId = dataset.id, savedSponsorship.id)
      )

      savedSponsorship shouldBe Sponsorship(
        dataset.id,
        Some("foo"),
        Some("bar"),
        None,
        savedSponsorship.id
      )

      val deleteResponse = client
        .removeDatasetSponsor(organizationId, datasetId, authToken)
        .awaitFinite()
        .value

      deleteResponse shouldBe RemoveDatasetSponsorResponse.NoContent

      val (_, _, indexedUpdatedSponsorship, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets(dataset.id)

      indexedUpdatedSponsorship shouldBe None

      intercept[NoSponsorshipForDatasetException] {
        ports.db
          .run(SponsorshipsMapper.getByDataset(dataset))
          .awaitFinite()
      }
    }
  }

  "Embargo request" should {
    "not Embargo with a release date before today" in {
      val expectedEmbargoReleaseDate = LocalDate.now().minusDays(1)

      val response = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(expectedEmbargoReleaseDate),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishStatus.PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe organizationId
      publicDataset.sourceDatasetId shouldBe datasetId
      publicDataset.ownerId shouldBe requestBody.ownerId
      publicDataset.ownerFirstName shouldBe requestBody.ownerFirstName
      publicDataset.ownerLastName shouldBe requestBody.ownerLastName
      publicDataset.ownerOrcid shouldBe requestBody.ownerOrcid

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion.version shouldBe 1
      publicVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      publicVersion.recordCount shouldBe requestBody.recordCount
      publicVersion.fileCount shouldBe requestBody.fileCount
      publicVersion.size shouldBe requestBody.size
      publicVersion.description shouldBe requestBody.description
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe config.s3.publishBucket
      publicVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${publicVersion.version}/"
      )
      publicVersion.doi shouldBe doiDto.doi
      publicVersion.embargoReleaseDate shouldBe None

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs.length shouldBe 1
      publishedJobs.head.s3Bucket shouldBe config.s3.publishBucket
    }

    "not Embargo with release date of today" in {
      val expectedEmbargoReleaseDate = LocalDate.now()

      val response = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(expectedEmbargoReleaseDate),
          requestBody,
          authToken
        )
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishStatus.PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version4
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset.name shouldBe requestBody.name
      publicDataset.sourceOrganizationId shouldBe organizationId
      publicDataset.sourceDatasetId shouldBe datasetId
      publicDataset.ownerId shouldBe requestBody.ownerId
      publicDataset.ownerFirstName shouldBe requestBody.ownerFirstName
      publicDataset.ownerLastName shouldBe requestBody.ownerLastName
      publicDataset.ownerOrcid shouldBe requestBody.ownerOrcid

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion.version shouldBe 1
      publicVersion.modelCount shouldBe Map[String, Long]("myConcept" -> 100L)
      publicVersion.recordCount shouldBe requestBody.recordCount
      publicVersion.fileCount shouldBe requestBody.fileCount
      publicVersion.size shouldBe requestBody.size
      publicVersion.description shouldBe requestBody.description
      publicVersion.status shouldBe PublishStatus.PublishInProgress
      publicVersion.s3Bucket shouldBe config.s3.publishBucket
      publicVersion.s3Key shouldBe S3Key.Version(
        s"${publicDataset.id}/${publicVersion.version}/"
      )
      publicVersion.doi shouldBe doiDto.doi
      publicVersion.embargoReleaseDate shouldBe None

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs.length shouldBe 1
      publishedJobs.head.s3Bucket shouldBe config.s3.publishBucket
    }

    "check Release Dates are after today" in {
      val yesterday = LocalDate.now().minusDays(1)
      val today = LocalDate.now()
      val tomorrow = LocalDate.now().plusDays(1)

      yesterday.isAfter(today) shouldBe false
      today.isAfter(today) shouldBe false
      tomorrow.isAfter(today) shouldBe true
    }
  }

  "Publishing Dataset Versions" should {
    "use the same S3 Bucket for all versions" in {
      val publishBucket = s"publish-bucket-${organizationId}"
      val embargoBucket = s"embargo-bucket-${organizationId}"
      val bucketConfig1 = definitions.BucketConfig(
        publish = publishBucket,
        embargo = embargoBucket
      )

      val datasetName = "this is a test dataset published multiple times"

      val requestBody1: definitions.PublishRequest = definitions.PublishRequest(
        name = datasetName,
        description = "A very very long description...",
        ownerId = 1,
        modelCount = Vector(definitions.ModelCount("myConcept", 100L)),
        recordCount = 100L,
        fileCount = 100L,
        size = 5555555L,
        license = License.`Apache 2.0`,
        contributors = Vector(internalContributor),
        externalPublications = Some(Vector(internalExternalPublication)),
        tags = Vector[String]("tag1", "tag2"),
        ownerNodeId = ownerNodeId,
        ownerFirstName = ownerFirstName,
        ownerLastName = ownerLastName,
        ownerOrcid = ownerOrcid,
        organizationNodeId = organizationNodeId,
        organizationName = organizationName,
        datasetNodeId = datasetNodeId,
        bucketConfig = Some(bucketConfig1),
        workflowId = Some(5)
      )

      // publish initial dataset version with bucket config {publish: A, embargo: B}
      val _ = client
        .publish(organizationId, datasetId, None, None, requestBody1, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val publicVersion1 = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      publicVersion1.version shouldBe 1
      publicVersion1.s3Bucket shouldBe S3Bucket(publishBucket)

      // "Complete" the publish job
      publishSuccessfully(publicDataset, publicVersion1)

      // publish next dataset version with bucket config {publish: C, embargo: D}
      val publishBucket2 = s"publish-bucket2-${organizationId}"
      val embargoBucket2 = s"embargo-bucket2-${organizationId}"
      val bucketConfig2 = definitions.BucketConfig(
        publish = publishBucket2,
        embargo = embargoBucket2
      )

      val requestBody2: definitions.PublishRequest = definitions.PublishRequest(
        name = datasetName,
        description = "A very very long description...",
        ownerId = 1,
        modelCount = Vector(definitions.ModelCount("myConcept", 100L)),
        recordCount = 100L,
        fileCount = 100L,
        size = 5555555L,
        license = License.`Apache 2.0`,
        contributors = Vector(internalContributor),
        externalPublications = Some(Vector(internalExternalPublication)),
        tags = Vector[String]("tag1", "tag2"),
        ownerNodeId = ownerNodeId,
        ownerFirstName = ownerFirstName,
        ownerLastName = ownerLastName,
        ownerOrcid = ownerOrcid,
        organizationNodeId = organizationNodeId,
        organizationName = organizationName,
        datasetNodeId = datasetNodeId,
        bucketConfig = Some(bucketConfig2),
        workflowId = Some(5)
      )

      val _ = client
        .publish(organizationId, datasetId, None, None, requestBody2, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishResponse.Created]
        .value

      val publicVersion2 = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      // check that datasetVersion1.s3Bucket == datasetVersion2.s3Bucket
      publicVersion2.version shouldBe 2
      publicVersion2.s3Bucket shouldBe S3Bucket(publishBucket)
    }

  }

}
