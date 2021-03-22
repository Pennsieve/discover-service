// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.alpakka.sqs.MessageAction
import akka.stream.scaladsl.{ Sink, Source }
import com.blackfynn.auth.middleware.Jwt
import com.blackfynn.discover.Authenticator.{
  generateServiceToken,
  generateUserToken
}
import com.blackfynn.discover._
import com.blackfynn.discover.client.definitions
import com.blackfynn.discover.client.definitions.{
  DatasetPublishStatus,
  InternalCollection,
  InternalContributor,
  SponsorshipRequest,
  SponsorshipResponse
}
import com.blackfynn.discover.client.publish._
import com.blackfynn.discover.clients._
import com.blackfynn.discover.db.profile.api._
import com.blackfynn.discover.db._
import com.blackfynn.discover.models._
import com.blackfynn.discover.notifications.{
  PublishNotification,
  SQSNotificationHandler
}
import com.blackfynn.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.blackfynn.models.{ Degree, License, PublishStatus, RelationshipType }
import com.blackfynn.test.EitherValue._
import io.circe.syntax._
import org.scalatest.{ Inside, Matchers, WordSpec }
import software.amazon.awssdk.services.sqs.model.Message
import java.time.LocalDate

import scala.concurrent.duration._

class PublishHandlerSpec
    extends WordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(PublishHandler.routes(ports))

  def createClient(routes: Route): PublishClient =
    PublishClient.httpClient(Route.asyncHandler(routes))

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

  val requestBody: definitions.PublishRequest = definitions.PublishRequest(
    name = datasetName,
    description = "A very very long description...",
    ownerId = 1,
    modelCount = IndexedSeq(definitions.ModelCount("myConcept", 100L)),
    recordCount = 100L,
    fileCount = 100L,
    size = 5555555L,
    license = License.`Apache 2.0`,
    contributors = IndexedSeq(internalContributor),
    externalPublications = Some(IndexedSeq(internalExternalPublication)),
    tags = IndexedSeq[String]("tag1", "tag2"),
    ownerNodeId = ownerNodeId,
    ownerFirstName = ownerFirstName,
    ownerLastName = ownerLastName,
    ownerOrcid = ownerOrcid,
    organizationNodeId = organizationNodeId,
    organizationName = organizationName,
    datasetNodeId = datasetNodeId
  )

  val reviseRequest = definitions.ReviseRequest(
    name = "A different name",
    description = "Brief and succint.",
    license = License.MIT,
    contributors = IndexedSeq(
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
      IndexedSeq(
        internalExternalPublication,
        new definitions.InternalExternalPublication(
          "10.26275/v62f-qd4v",
          Some(RelationshipType.IsReferencedBy)
        )
      )
    ),
    tags = IndexedSeq[String]("red", "green", "blue"),
    ownerId = 99999,
    ownerFirstName = "Haskell",
    ownerLastName = "Curry",
    ownerOrcid = "0000-1111-1111-1111",
    bannerPresignedUrl =
      "https://s3.amazonaws.localhost/dev-dataset-assets-use1/banner.jpg",
    readmePresignedUrl =
      "https://s3.amazonaws.localhost/dev-dataset-assets-use1/readme.md",
    collections = Some(
      IndexedSeq[InternalCollection](
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
        None
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset should have(
        'name (requestBody.name),
        'sourceOrganizationId (organizationId),
        'sourceDatasetId (datasetId),
        'ownerId (requestBody.ownerId),
        'ownerFirstName (requestBody.ownerFirstName),
        'ownerLastName (requestBody.ownerLastName),
        'ownerOrcid (requestBody.ownerOrcid)
      )

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion should have(
        'version (1),
        'modelCount (Map[String, Long]("myConcept" -> 100L)),
        'recordCount (requestBody.recordCount),
        'fileCount (requestBody.fileCount),
        'size (requestBody.size),
        'description (requestBody.description),
        'status (PublishStatus.PublishInProgress),
        's3Bucket ("bucket"),
        's3Key (s"${publicDataset.id}/${publicVersion.version}/"),
        'doi (doi.doi)
      )

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
          job should have(
            'organizationId (organizationId),
            'organizationNodeId (organizationNodeId),
            'organizationName (organizationName),
            'datasetId (datasetId),
            'datasetNodeId (datasetNodeId),
            'publishedDatasetId (publicDataset.id),
            'userId (requestBody.ownerId),
            'userNodeId (requestBody.ownerNodeId),
            'userFirstName (requestBody.ownerFirstName),
            'userLastName (requestBody.ownerLastName),
            'userOrcid (requestBody.ownerOrcid),
            's3PgdumpKey (
              S3Key
                .Version(publicDataset.id, publicVersion.version) / "dump.sql" toString
            ),
            's3PublishKey (
              S3Key.Version(publicDataset.id, publicVersion.version) toString
            ),
            'version (publicVersion.version),
            'doi (doi.doi)
          )
          inside(job.contributors) {
            case contributor :: Nil =>
              contributor should have(
                'datasetId (publicDataset.id),
                'versionId (publicVersion.version),
                'firstName ("Sally"),
                'lastName ("Field"),
                'middleInitial (Some("M")),
                'degree (Some(Degree.BS)),
                'orcid (None),
                'sourceContributorId (1),
                'sourceUserId (None)
              )
          }

          inside(job.externalPublications) {
            case externalPublication :: Nil =>
              externalPublication should have(
                'doi ("10.26275/t6j6-77pu"),
                'relationshipType (RelationshipType.Describes)
              )
          }
      }
    }

    "publish to an embargo bucket" in {
      val embargoReleaseDate = LocalDate.of(2025, 6, 1)

      val response = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(embargoReleaseDate),
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
        None
      )

      val publicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      publicDataset should have(
        'name (requestBody.name),
        'sourceOrganizationId (organizationId),
        'sourceDatasetId (datasetId),
        'ownerId (requestBody.ownerId),
        'ownerFirstName (requestBody.ownerFirstName),
        'ownerLastName (requestBody.ownerLastName),
        'ownerOrcid (requestBody.ownerOrcid)
      )

      val publicVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion should have(
        'version (1),
        'modelCount (Map[String, Long]("myConcept" -> 100L)),
        'recordCount (requestBody.recordCount),
        'fileCount (requestBody.fileCount),
        'size (requestBody.size),
        'description (requestBody.description),
        'status (PublishStatus.EmbargoInProgress),
        's3Bucket ("embargo-bucket"),
        's3Key (s"${publicDataset.id}/${publicVersion.version}/"),
        'doi (doi.doi),
        'embargoReleaseDate (Some(embargoReleaseDate))
      )

      val publishedJobs = ports.stepFunctionsClient
        .asInstanceOf[MockStepFunctionsClient]
        .startedJobs

      publishedJobs.length shouldBe (1)
      publishedJobs.head.s3Bucket shouldBe (S3Bucket("embargo-bucket"))
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
        Some(publicDataset1_V1.createdAt)
      )

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      publicVersion should have(
        'version (2),
        'modelCount (Map[String, Long]("myConcept" -> 100L)),
        'recordCount (requestBody.recordCount),
        'fileCount (requestBody.fileCount),
        'size (requestBody.size),
        'status (PublishStatus.PublishInProgress),
        's3Bucket ("bucket"),
        's3Key (s"${publicDataset.id}/${publicVersion.version}/"),
        'doi (doi.doi)
      )
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
        None
      )

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      latestVersion should have(
        'version (1),
        'modelCount (Map[String, Long]("myConcept" -> 100L)),
        'recordCount (requestBody.recordCount),
        'fileCount (requestBody.fileCount),
        'size (requestBody.size),
        'status (PublishStatus.PublishInProgress),
        's3Bucket ("bucket"),
        's3Key (s"${publicDataset.id}/${latestVersion.version}/"),
        'doi (doi.doi)
      )
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
        None
      )

      val doi = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      latestVersion should have(
        'version (1),
        'modelCount (Map[String, Long]("myConcept" -> 100L)),
        'recordCount (requestBody.recordCount),
        'fileCount (requestBody.fileCount),
        'size (requestBody.size),
        'status (PublishStatus.PublishInProgress),
        's3Bucket ("bucket"),
        's3Key (s"${publicDataset.id}/${latestVersion.version}/"),
        'doi (doi.doi)
      )
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

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = Unpublished
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
        Some(version.createdAt)
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
      revisedDataset should have(
        'name ("A different name"),
        'license (License.MIT),
        'tags (List("red", "green", "blue")),
        'ownerId (99999),
        'ownerFirstName ("Haskell"),
        'ownerLastName ("Curry"),
        'ownerOrcid ("0000-1111-1111-1111")
      )
      revisedVersion should have(
        'status (PublishSucceeded),
        'description ("Brief and succint."),
        'size (76543 + 300), // From publish notification + new files
        'banner (
          Some(
            revisedVersion.s3Key / s"revisions/${revision.revision}/banner.jpg"
          )
        ),
        'readme (
          Some(
            revisedVersion.s3Key / s"revisions/${revision.revision}/readme.md"
          )
        )
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
              revisedVersion.s3Key / s"revisions/${revision.revision}/banner.jpg"
            )
          )
          .sortBy(_.name)
          .result
      )

      createdFiles.map(f => (f.name, f.fileType)) shouldBe List(
        ("banner.jpg", "JPEG"),
        ("manifest.json", "Json"),
        ("readme.md", "Markdown")
      )

      // Reindexes the dataset with the revision
      val (indexedVersion, indexedRevision, _, _, _) = ports.searchClient
        .asInstanceOf[MockSearchClient]
        .indexedDatasets
        .get(revisedDataset.id)
        .get
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
        Some(version.createdAt)
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
        .indexedDatasets
        .get(revisedDataset.id)
        .get
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
        .release(organizationId, datasetId)
        .awaitFinite()
        .value

      response shouldBe ReleaseResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .release(organizationId, datasetId, userAuthToken)
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
        .release(organizationId, datasetId, authToken)
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
        .release(organizationId, datasetId, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[ReleaseResponse.Forbidden]
        .value
    }

    "fail to release a dataset that does not exist" in {

      val response = client
        .release(organizationId, datasetId, authToken)
        .awaitFinite()
        .value shouldBe ReleaseResponse.NotFound
    }

    "release an embargoed dataset" in {
      TestUtilities.createDatasetV1(ports.db)(
        name = datasetName,
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.EmbargoSucceeded
      )

      val response = client
        .release(organizationId, datasetId, authToken)
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
        Some(version.createdAt)
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
            s3Key = version.s3Key
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
        .release(organizationId, datasetId, authToken)
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
        .unpublish(organizationId, datasetId)
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Unauthorized
    }

    "fail with a user JWT" in {
      val response = client
        .unpublish(organizationId, datasetId, userAuthToken)
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
          .unpublish(organizationId, datasetId, authToken)
          .awaitFinite()
          .value
          .asInstanceOf[UnpublishResponse.OK]
          .value

      response.status shouldBe Unpublished

      ports.lambdaClient
        .asInstanceOf[MockLambdaClient]
        .s3Keys shouldBe List(publicDataset.id.toString)

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
        .unpublish(organizationId, datasetId, authToken)
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
        None
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === version.datasetId)
          .result
      ) shouldBe empty
    }

    "rollback an embargoed version when unpublishing" in {

      val status = client
        .publish(
          organizationId,
          datasetId,
          Some(true),
          Some(LocalDate.now),
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
        .unpublish(organizationId, datasetId, authToken)
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
        None
      )

      run(
        PublicDatasetVersionsMapper
          .filter(_.datasetId === version.datasetId)
          .result
      ) shouldBe empty
    }

    "fail to unpublish a dataset that is currently publishing" in {

      TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = organizationId,
        sourceDatasetId = datasetId,
        status = PublishStatus.PublishInProgress
      )

      val response = client
        .unpublish(organizationId, datasetId, authToken)
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
        .unpublish(organizationId, datasetId, authToken)
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
        .unpublish(organizationId, datasetId, authToken)
        .awaitFinite()
        .value

      response shouldBe UnpublishResponse.Forbidden(
        "Cannot unpublish a dataset that is being published"
      )
    }

    "respond with NoContent for a dataset that was never published" in {

      val response =
        client
          .unpublish(organizationId, datasetId, authToken)
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
        Some(publicDatasetV1.createdAt)
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

      val expected = IndexedSeq(
        DatasetPublishStatus(
          publicDataset1.name,
          publicDataset1.sourceOrganizationId,
          publicDataset1.sourceDatasetId,
          Some(publicDataset1.id),
          1,
          PublishSucceeded,
          Some(publicDataset1_V1.createdAt)
        ),
        DatasetPublishStatus(
          publicDataset2.name,
          publicDataset2.sourceOrganizationId,
          publicDataset2.sourceDatasetId,
          Some(publicDataset2.id),
          1,
          PublishInProgress,
          Some(publicDataset2_V1.createdAt),
          Some(SponsorshipRequest(Some("foo"), Some("bar"), None))
        )
      )

      response shouldBe GetStatusesResponse.OK(expected)
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
        .indexedDatasets
        .get(dataset.id)
        .get

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
        .indexedDatasets
        .get(dataset.id)
        .get

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
        .indexedDatasets
        .get(dataset.id)
        .get

      indexedUpdatedSponsorship shouldBe None

      intercept[NoSponsorshipForDatasetException] {
        ports.db
          .run(SponsorshipsMapper.getByDataset(dataset))
          .awaitFinite()
      }
    }
  }

}
