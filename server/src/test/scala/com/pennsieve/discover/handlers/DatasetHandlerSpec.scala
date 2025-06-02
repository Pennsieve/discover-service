// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import java.nio.file.Path
import java.time.LocalDate
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethods,
  HttpRequest,
  HttpResponse,
  StatusCode,
  StatusCodes
}
import akka.http.scaladsl.model.headers.{
  `Content-Disposition`,
  Authorization,
  ContentDispositionTypes,
  OAuth2BearerToken
}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.{ RouteTestTimeout, ScalatestRouteTest }
import akka.stream.scaladsl._
import akka.testkit.TestDuration
import akka.util.ByteString
import com.pennsieve.discover.TestUtilities._
import com.pennsieve.discover._
import com.pennsieve.discover.client.dataset._
import com.pennsieve.discover.client.definitions.{
  DatasetsPage,
  DownloadRequest,
  PreviewAccessRequest
}
import com.pennsieve.discover.clients.{
  DatasetPreview,
  HttpError,
  MockAuthorizationClient,
  MockS3StreamClient,
  TestFile,
  User
}
import com.pennsieve.discover.db.{
  DatasetDownloadsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  SponsorshipsMapper
}
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions.{
  AssetTreePage,
  SponsorshipDto
}
import com.pennsieve.models.PublishStatus.{
  EmbargoSucceeded,
  NotPublished,
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.pennsieve.models.{
  DatasetType,
  FileType,
  Icon,
  PackageType,
  PublishStatus,
  RelationshipType
}
import com.pennsieve.test.EitherValue._
import io.circe._
import io.circe.syntax._
import io.scalaland.chimney.dsl._
import squants.information.Megabytes

import java.time.LocalDate
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.{
  generateServiceClaim,
  generateServiceToken,
  generateUserToken
}
import com.pennsieve.models.RelationshipType.{ Documents, References, Requires }
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DatasetHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness
    with TempDirectoryFixture {

  val organizationId = 1
  val datasetId = 2
  val userId = 1

  val token: Jwt.Token =
    generateUserToken(
      ports.jwt,
      userId = userId,
      organizationId = organizationId,
      datasetId = Some(datasetId)
    )

  val authToken = List(Authorization(OAuth2BearerToken(token.value)))

  def createRoutes(): Route =
    Route.seal(DatasetHandler.routes(ports))

  def createClient(routes: Route): DatasetClient =
    DatasetClient.httpClient(Route.toFunction(routes))

  def toClientDefinition(
    dto: server.definitions.PublicDatasetDto
  ): client.definitions.PublicDatasetDto =
    dto
      .into[client.definitions.PublicDatasetDto]
      .transform

  val datasetClient: DatasetClient = createClient(createRoutes())

  /**
    * Reusible collection of tests cases to validate that endpoints which can
    * access restricted data behave properly when datasets are under embargo.
    *
    * See https://www.scalatest.org/user_guide/sharing_tests for more details on
    * how these tests are set up and used.
    */
  def AccessIsRestrictedForEmbargoedData(
    request: PublicDatasetVersion => HttpRequest
  ): Unit = {

    "optionally validate a JWT" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )
      addMetadata(ports.db, v1)

      request(v1) ~> addHeader(
        Authorization(
          OAuth2BearerToken(
            Authenticator.generateUserToken(ports.jwt, 1, 1, Some(1)).value
          )
        )
      ) ~>
        createRoutes() ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "reject an expired JWT" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.EmbargoSucceeded
      )

      request(v1) ~> addHeader(
        Authorization(
          OAuth2BearerToken(
            Authenticator
              .generateUserToken(ports.jwt, 1, 1, Some(1), 0.seconds)
              .value
          )
        )
      ) ~>
        createRoutes() ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "return 401 error if the dataset is under embargo and user is not authenticated" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.EmbargoSucceeded
      )

      request(v1) ~>
        createRoutes() ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "return 403 error if the dataset is under embargo and user is not authorized" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.EmbargoSucceeded
      )

      request(v1) ~> addHeader(
        ports.authorizationClient
          .asInstanceOf[MockAuthorizationClient]
          .forbiddenHeader
      ) ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "return 200 if the dataset is under embargo and user is authorized" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.EmbargoSucceeded,
        migrated = true
      )
      addMetadata(ports.db, v1)

      request(v1) ~> addHeader(
        ports.authorizationClient
          .asInstanceOf[MockAuthorizationClient]
          .authorizedHeader
      ) ~>
        createRoutes() ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }

  "PublicDatasetDTO" should {
    "be encoded correctly" in {
      val publicDataset =
        TestUtilities.createDataset(ports.db)()
      val today = LocalDate.now
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded,
          embargoReleaseDate = Some(today)
        )
      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = version.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val revision = TestUtilities.createRevision(ports.db)(version)

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset.id,
        version = version.version,
        sourceCollectionId = 1
      )

      val externalPublication = PublicExternalPublication(
        doi = "10.26275/v62f-qd4v",
        relationshipType = RelationshipType.Describes,
        datasetId = datasetId,
        version = version.version
      )

      val dto =
        PublicDatasetDTO(
          publicDataset,
          version,
          Seq(contributor),
          None,
          Some(revision),
          Seq(collection),
          Seq(externalPublication),
          None
        )

      dto.banner shouldBe Some(
        "https://assets.discover.pennsieve.org/dataset-assets/path-to-banner"
      )
      dto.readme shouldBe Some(
        "https://assets.discover.pennsieve.org/dataset-assets/path-to-readme"
      )
      dto.arn shouldBe s"arn:aws:s3:::bucket/${version.s3Key}"
      dto.uri shouldBe s"s3://bucket/${version.s3Key}"
      dto.pennsieveSchemaVersion shouldBe Some("4.0")

      dto.firstPublishedAt shouldBe Some(publicDataset.createdAt)
      dto.versionPublishedAt shouldBe Some(version.createdAt)
      dto.revisedAt shouldBe Some(revision.createdAt)
      dto.embargoReleaseDate shouldBe Some(today)
    }
  }

  "GET /dataset/{id}" should {
    "return a published dataset" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = publicDatasetV1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      ports.db.run(
        SponsorshipsMapper.createOrUpdate(
          publicDataset.sourceOrganizationId,
          publicDataset.sourceDatasetId,
          Some("foo"),
          Some("bar"),
          Some("baz")
        )
      )

      val dataset: PublicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDataset(publicDataset.id)
        )
        .await

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset.id,
        version = publicDatasetV1.version,
        sourceCollectionId = 1
      )

      // If an authorization header is present, include the corresponding dataset preview:
      val expectedWithAuth = models.PublicDatasetDTO(
        dataset,
        publicDatasetV1,
        IndexedSeq(PublicContributorDTO.apply(contributor)),
        Some(SponsorshipDto(Some("foo"), Some("bar"), Some("baz"))),
        None,
        Some(IndexedSeq(PublicCollectionDTO.apply(collection))),
        Some(IndexedSeq.empty),
        Some(
          DatasetPreview(
            user = User("N:user:1", "Joe Schmo", 1),
            embargoAccess = "Requested"
          )
        ),
        None,
        None
      )

      val responseWithAuth =
        datasetClient
          .getDataset(publicDataset.id, headers = authToken)
          .awaitFinite()
          .value

      responseWithAuth shouldBe GetDatasetResponse.OK(
        toClientDefinition(expectedWithAuth)
      )

      // If an authorization header is present, include the corresponding dataset preview:
      val expectedNoAuth = models.PublicDatasetDTO(
        dataset,
        publicDatasetV1,
        IndexedSeq(PublicContributorDTO.apply(contributor)),
        Some(SponsorshipDto(Some("foo"), Some("bar"), Some("baz"))),
        None,
        Some(IndexedSeq(PublicCollectionDTO.apply(collection))),
        Some(IndexedSeq.empty),
        None,
        None,
        None
      )

      val responseNoAuth =
        datasetClient
          .getDataset(publicDataset.id)
          .awaitFinite()
          .value

      responseNoAuth shouldBe GetDatasetResponse.OK(
        toClientDefinition(expectedNoAuth)
      )
    }

    "fail if the dataset is not found" in {

      val response =
        datasetClient.getDataset(5, headers = authToken).awaitFinite().value

      response shouldBe GetDatasetResponse.NotFound("5")
    }

    "return 410 tombstone if the dataset has been unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          dataset.id,
          status = PublishStatus.Unpublished
        )

      val response = datasetClient
        .getDataset(dataset.id, headers = authToken)
        .awaitFinite()
        .value

      response shouldBe GetDatasetResponse.Gone(
        client.definitions.TombstoneDto(
          id = dataset.id,
          version = version.version,
          name = dataset.name,
          tags = dataset.tags.toVector,
          status = PublishStatus.Unpublished,
          doi = version.doi,
          updatedAt = dataset.updatedAt
        )
      )
    }
  }

  "GET /dataset/doi/{prefix}/{suffix}" should {

    "get a dataset" in {

      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishSucceeded,
          doi = "10.12345/abcd-efgh"
        )

      val response: GetDatasetByDoiResponse = datasetClient
        .getDatasetByDoi("10.12345", "abcd-efgh")
        .awaitFinite()
        .value

      response shouldBe GetDatasetByDoiResponse.OK(
        toClientDefinition(
          models
            .PublicDatasetDTO(
              dataset,
              version,
              List.empty,
              None,
              None,
              List.empty,
              List.empty,
              None
            )
        )
      )
    }

    "return 410 tombstone if the dataset has been unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishStatus.Unpublished,
          doi = "10.12345/abcd-efgh"
        )

      val response: GetDatasetByDoiResponse = datasetClient
        .getDatasetByDoi("10.12345", "abcd-efgh")
        .awaitFinite()
        .value

      response shouldBe GetDatasetByDoiResponse.Gone(
        client.definitions.TombstoneDto(
          id = dataset.id,
          version = version.version,
          name = dataset.name,
          tags = dataset.tags.toVector,
          status = PublishStatus.Unpublished,
          doi = "10.12345/abcd-efgh",
          updatedAt = dataset.updatedAt
        )
      )
    }
  }

  "GET /datasets/doi" should {
    "get dataset DTOs and Tombstone DTOs" in {
      val doiPrefix = "10.9999.9"
      def randomDoi() = s"$doiPrefix/${randomString()}"

      val ds1 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 1, name = "A")
      val ds2 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 2, name = "B")
      val ds3 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 3, name = "C")
      val ds4 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 4, name = "D")
      val ds1_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded,
        doi = randomDoi()
      )
      val ds2_v1_failed = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds2.id,
        status = PublishFailed,
        size = 100L,
        doi = randomDoi()
      )
      val ds3_v1_embargoed = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds3.id,
        status = EmbargoSucceeded,
        size = 90L,
        doi = randomDoi()
      )
      val ds4_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds4.id,
        status = PublishSucceeded,
        size = 80L,
        doi = randomDoi()
      )
      val ds1_v2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded,
        size = 70L,
        doi = randomDoi()
      )

      val ds1_v1_contrib = TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = ds1.id,
        organizationId = 1,
        version = ds1_v1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      TestUtilities.createContributor(ports.db)(
        firstName = "Tom",
        lastName = "Hanks",
        orcid = None,
        datasetId = ds2.id,
        organizationId = 1,
        version = ds2_v1_failed.version,
        sourceContributorId = 2,
        sourceUserId = Some(2)
      )
      val ds3_v1_embargoed_contrib = TestUtilities.createContributor(ports.db)(
        firstName = "Tony",
        lastName = "Parker",
        orcid = None,
        datasetId = ds3.id,
        organizationId = 1,
        version = ds3_v1_embargoed.version,
        sourceContributorId = 3,
        sourceUserId = Some(3)
      )
      val ds4_v1_contrib = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = ds4.id,
        organizationId = 1,
        version = ds4_v1.version,
        sourceContributorId = 4,
        sourceUserId = Some(4)
      )
      val ds1_v2_contrib = TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = ds1.id,
        organizationId = 1,
        version = ds1_v2.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val ds1_sponsorship = TestUtilities.createSponsorship(ports.db)(
        sourceOrganizationId = ds1.sourceOrganizationId,
        sourceDatasetId = ds1.sourceDatasetId
      )

      val ds4_v1_revision = TestUtilities.createRevision(ports.db)(ds4_v1)

      val ds1_v2_collection = TestUtilities.createCollection(ports.db)(
        datasetId = ds1_v2.datasetId,
        version = ds1_v2.version,
        sourceCollectionId = 1
      )

      val ds4_v1_release = TestUtilities.createDatasetRelease(ports.db)(
        ds4_v1.datasetId,
        ds4_v1.version,
        "GitHub",
        "v1.0.0",
        "https://github.com/Pennsieve/test-repo"
      )

      val ds1_v1_externalPub = TestUtilities.createExternalPublication(
        ports.db
      )(ds1_v1.datasetId, ds1_v1.version, References)

      val ds4_v1_externalPub1 = TestUtilities.createExternalPublication(
        ports.db
      )(ds4_v1.datasetId, ds4_v1.version, Requires)

      val ds4_v1_externalPub2 = TestUtilities.createExternalPublication(
        ports.db
      )(ds4_v1.datasetId, ds4_v1.version, Documents)

      val response = datasetClient
        .getDatasetsByDoi(
          List(
            ds1_v1.doi,
            ds1_v2.doi,
            ds2_v1_failed.doi,
            ds3_v1_embargoed.doi,
            ds4_v1.doi
          )
        )
        .awaitFinite()
        .value

      inside(response) {
        case GetDatasetsByDoiResponse.OK(result) =>
          // Check Unpublished
          result.unpublished.size shouldBe 1

          result.unpublished should contain key ds2_v1_failed.doi
          result.unpublished(ds2_v1_failed.doi) shouldBe client.definitions
            .TombstoneDto(
              id = ds2_v1_failed.datasetId,
              version = ds2_v1_failed.version,
              name = ds2.name,
              tags = ds2.tags.toVector,
              status = PublishStatus.PublishFailed,
              doi = ds2_v1_failed.doi,
              updatedAt = ds2.updatedAt
            )

          // Check published
          result.published.size shouldBe 4

          result.published should contain key ds1_v1.doi
          result.published(ds1_v1.doi) shouldBe toClientDefinition(
            models.PublicDatasetDTO(
              dataset = ds1,
              version = ds1_v1,
              contributors = Seq(ds1_v1_contrib),
              sponsorship = Some(ds1_sponsorship),
              revision = None,
              collections = Seq.empty,
              externalPublications = Seq(ds1_v1_externalPub),
              datasetPreview = None,
              release = None
            )
          )

          result.published should contain key ds1_v2.doi
          result.published(ds1_v2.doi) shouldBe toClientDefinition(
            models.PublicDatasetDTO(
              dataset = ds1,
              version = ds1_v2,
              contributors = IndexedSeq(ds1_v2_contrib),
              sponsorship = Some(ds1_sponsorship),
              revision = None,
              collections = IndexedSeq(ds1_v2_collection),
              externalPublications = IndexedSeq.empty,
              datasetPreview = None,
              release = None
            )
          )

          result.published should contain key ds3_v1_embargoed.doi
          result.published(ds3_v1_embargoed.doi) shouldBe toClientDefinition(
            models.PublicDatasetDTO(
              dataset = ds3,
              version = ds3_v1_embargoed,
              contributors = IndexedSeq(ds3_v1_embargoed_contrib),
              sponsorship = None,
              revision = None,
              collections = IndexedSeq.empty,
              externalPublications = IndexedSeq.empty,
              datasetPreview = None,
              release = None
            )
          )

          // Breaking this one up because the external pubs are not returned in a deterministic order
          result.published should contain key ds4_v1.doi
          val ds4_v1_result = result.published(ds4_v1.doi)

          // only difference between these two should be the order of the external publications
          val ds4_v1_expectedServerDef1 = models.PublicDatasetDTO(
            dataset = ds4,
            version = ds4_v1,
            contributors = IndexedSeq(ds4_v1_contrib),
            sponsorship = None,
            revision = Some(ds4_v1_revision),
            collections = IndexedSeq.empty,
            externalPublications =
              IndexedSeq(ds4_v1_externalPub1, ds4_v1_externalPub2),
            datasetPreview = None,
            release = Some(ds4_v1_release)
          )
          val ds4_v1_expectedServerDef2 = models.PublicDatasetDTO(
            dataset = ds4,
            version = ds4_v1,
            contributors = IndexedSeq(ds4_v1_contrib),
            sponsorship = None,
            revision = Some(ds4_v1_revision),
            collections = IndexedSeq.empty,
            externalPublications =
              IndexedSeq(ds4_v1_externalPub2, ds4_v1_externalPub1),
            datasetPreview = None,
            release = Some(ds4_v1_release)
          )

          ds4_v1_result should (be(
            toClientDefinition(ds4_v1_expectedServerDef1)
          ) or be(toClientDefinition(ds4_v1_expectedServerDef2)))
      }
    }

    "fail if there are no doi query params" in {
      val response =
        datasetClient.getDatasetsByDoi(doi = Nil).awaitFinite().value

      response shouldBe GetDatasetsByDoiResponse.BadRequest("missing DOIs")
    }
  }

  "POST /datasets/{datasetId}/preview" should {
    "request preview access to a dataset" in {

      val publicDataset = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 99,
        sourceDatasetId = 100
      )

      var response: RequestPreviewResponse = datasetClient
        .requestPreview(
          datasetId = publicDataset.id,
          body = PreviewAccessRequest(dataUseAgreementId = Some(1))
        )
        .awaitFinite()
        .value
      response shouldBe RequestPreviewResponse.Unauthorized("missing token")

      response = datasetClient
        .requestPreview(
          datasetId = publicDataset.id,
          body = PreviewAccessRequest(dataUseAgreementId = Some(1)),
          headers = authToken
        )
        .awaitFinite()
        .value
      response shouldBe RequestPreviewResponse.OK

      response = datasetClient
        .requestPreview(
          datasetId = publicDataset.id,
          body = PreviewAccessRequest(dataUseAgreementId = None),
          headers = authToken
        )
        .awaitFinite()
        .value
      response shouldBe RequestPreviewResponse.OK

      response = datasetClient
        .requestPreview(
          datasetId = 999,
          body = PreviewAccessRequest(dataUseAgreementId = None),
          headers = authToken
        )
        .awaitFinite()
        .value
      response shouldBe RequestPreviewResponse.NotFound("999")

      val badResponse = datasetClient
        .requestPreview(
          datasetId = publicDataset.id,
          body = PreviewAccessRequest(dataUseAgreementId = Some(999)),
          headers = authToken
        )
        .awaitFinite()
        .value
      badResponse shouldBe RequestPreviewResponse.NotFound(
        "data use agreement not found"
      )
    }
  }

  "GET /dataset/{id}/versions" should {
    "return the published versions of a dataset" in {

      val publicDataset = TestUtilities.createDataset(ports.db)()
      val version1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = Unpublished
        )
      val version2 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )
      val version3 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishInProgress
      )
      val version4 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = EmbargoSucceeded
      )

      val contributorV1 = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = version1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val contributorV2 = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = version2.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val response: GetDatasetVersionsResponse = datasetClient
        .getDatasetVersions(publicDataset.id)
        .awaitFinite()
        .value

      response shouldBe GetDatasetVersionsResponse.OK(
        Vector(
          PublicDatasetDTO.apply(
            publicDataset,
            version4,
            Seq.empty,
            None,
            None,
            Seq.empty,
            Seq.empty,
            None
          ),
          PublicDatasetDTO.apply(
            publicDataset,
            version2,
            Seq(contributorV2),
            None,
            None,
            Seq.empty,
            Seq.empty,
            None
          ),
          PublicDatasetDTO.apply(
            publicDataset,
            version1,
            Seq(contributorV1),
            None,
            None,
            Seq.empty,
            Seq.empty,
            None
          )
        ).map(toClientDefinition)
      )
    }

    "fail if the dataset is not found" in {

      val response =
        datasetClient.getDatasetVersions(12345).awaitFinite().value

      response shouldBe GetDatasetVersionsResponse.NotFound("12345")
    }

  }

  "GET /dataset/{id}/versions/{versionId}" should {
    "return a dataset version" in {

      val publicDataset = TestUtilities.createDataset(ports.db)()
      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val dataset: PublicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDataset(publicDataset.id)
        )
        .await

      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = publicDatasetV1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val contributor2 = TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = publicDataset.id,
        organizationId = organizationId,
        version = publicDatasetV1.version,
        sourceContributorId = 2,
        sourceUserId = Some(2)
      )

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset.id,
        version = publicDatasetV1.version,
        sourceCollectionId = 1
      )

      val expected = models.PublicDatasetDTO(
        dataset,
        publicDatasetV1,
        List(contributor, contributor2),
        None,
        None,
        List(collection),
        List.empty,
        None,
        None
      )

      val response =
        datasetClient.getDatasetVersion(publicDataset.id, 1).awaitFinite().value

      response shouldBe GetDatasetVersionResponse
        .OK(toClientDefinition(expected))

    }

    "fail if the dataset version is not found" in {
      val publicDataset = TestUtilities.createDataset(ports.db)()
      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(publicDataset.id)

      val response =
        datasetClient.getDatasetVersion(publicDataset.id, 5).awaitFinite().value

      response shouldBe GetDatasetVersionResponse.NotFound(
        publicDataset.id.toString
      )
    }

    "return 410 tombstone if the dataset has been unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          dataset.id,
          status = PublishStatus.Unpublished
        )

      val response = datasetClient
        .getDatasetVersion(dataset.id, version.version)
        .awaitFinite()
        .value

      response shouldBe GetDatasetVersionResponse.Gone(
        client.definitions.TombstoneDto(
          id = dataset.id,
          version = version.version,
          name = dataset.name,
          tags = dataset.tags.toVector,
          status = PublishStatus.Unpublished,
          doi = version.doi,
          updatedAt = dataset.updatedAt
        )
      )
    }
  }

  "GET /datasets" should {
    "return the latest version of each published dataset" in {
      // Dataset 1
      val publicDataset1 = TestUtilities.createDataset(ports.db)()
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.NotPublished
        )
      val publicDataset1_V2: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 2
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )

      // Dataset 3 (embargoed)
      val publicDataset3 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 3
      )
      val publicDataset3_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset3.id,
          status = PublishStatus.EmbargoSucceeded
        )

      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = organizationId,
        version = publicDataset1_V1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val contributor2 = TestUtilities.createContributor(ports.db)(
        firstName = "Tony",
        lastName = "Parker",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = organizationId,
        version = publicDataset1_V1.version,
        sourceContributorId = 2,
        sourceUserId = Some(2)
      )

      val contributor3 = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = organizationId,
        version = publicDataset1_V2.version,
        sourceContributorId = 1,
        sourceUserId = Some(3)
      )

      val contributor4 = TestUtilities.createContributor(ports.db)(
        firstName = "Tony",
        lastName = "Parker",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = organizationId,
        version = publicDataset1_V2.version,
        sourceContributorId = 2,
        sourceUserId = Some(4)
      )

      val contributor5 = TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = publicDataset2.id,
        organizationId = organizationId,
        version = publicDataset2_V1.version,
        sourceContributorId = 3,
        sourceUserId = Some(5)
      )

      val collection1 = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset1.id,
        version = publicDataset1_V2.version,
        sourceCollectionId = 1
      )

      val collection2 = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset2.id,
        version = publicDataset2_V1.version,
        sourceCollectionId = 1
      )

      val collection3 = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset3.id,
        version = publicDataset3_V1.version,
        sourceCollectionId = 1
      )

      ports.db
        .run(
          SponsorshipsMapper.createOrUpdate(
            publicDataset2.sourceOrganizationId,
            publicDataset2.sourceDatasetId,
            Some("foo"),
            Some("bar"),
            Some("baz")
          )
        )
        .awaitFinite()

      val embargoedDatasets = IndexedSeq(
        toClientDefinition(
          PublicDatasetDTO.apply(
            publicDataset3,
            publicDataset3_V1,
            IndexedSeq(),
            None: Option[SponsorshipDto],
            None,
            Some(IndexedSeq(PublicCollectionDTO(collection3))),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        )
      )

      val nonEmbargoedDatasets = IndexedSeq(
        toClientDefinition(
          PublicDatasetDTO.apply(
            publicDataset2,
            publicDataset2_V1,
            IndexedSeq(PublicContributorDTO.apply(contributor5)),
            Some(SponsorshipDto(Some("foo"), Some("bar"), Some("baz"))),
            None,
            Some(IndexedSeq(PublicCollectionDTO(collection2))),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        ),
        toClientDefinition(
          PublicDatasetDTO(
            publicDataset1,
            publicDataset1_V2,
            IndexedSeq(contributor3, contributor4)
              .map(PublicContributorDTO.apply(_)),
            None,
            None,
            Some(IndexedSeq(PublicCollectionDTO(collection1))),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        )
      )

      val allDatasets = embargoedDatasets ++ nonEmbargoedDatasets

      // If "embargo=" is omitted, return all datasets:
      assert(
        datasetClient
          .getDatasets(tags = None)
          .awaitFinite()
          .value === GetDatasetsResponse.OK(
          DatasetsPage(
            limit = 10,
            offset = 0,
            datasets = allDatasets.toVector,
            totalCount = allDatasets.size.toLong
          )
        )
      )

      assert(
        datasetClient
          .getDatasets(tags = None, embargo = Some(true))
          .awaitFinite()
          .value === GetDatasetsResponse.OK(
          DatasetsPage(
            limit = 10,
            offset = 0,
            datasets = embargoedDatasets.toVector,
            totalCount = embargoedDatasets.size.toLong
          )
        )
      )

      assert(
        datasetClient
          .getDatasets(tags = None, embargo = Some(false))
          .awaitFinite()
          .value === GetDatasetsResponse.OK(
          DatasetsPage(
            limit = 10,
            offset = 0,
            datasets = nonEmbargoedDatasets.toVector,
            totalCount = nonEmbargoedDatasets.size.toLong
          )
        )
      )
    }

    "return the latest version of each published dataset in the list" in {
      // Dataset 1

      val publicDataset1 = TestUtilities.createDataset(ports.db)(name = "un")
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.NotPublished
        )
      val publicDataset1_V2: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 2,
        name = "deux"
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )

      ports.db
        .run(
          SponsorshipsMapper.createOrUpdate(
            publicDataset2.sourceOrganizationId,
            publicDataset2.sourceDatasetId,
            Some("foo"),
            Some("bar"),
            Some("baz")
          )
        )
        .awaitFinite()

      val expected = DatasetsPage(
        limit = 10,
        offset = 0,
        datasets = Vector(
          toClientDefinition(
            PublicDatasetDTO.apply(
              publicDataset2,
              publicDataset2_V1,
              Seq.empty,
              Some(
                Sponsorship(
                  publicDataset2.id,
                  Some("foo"),
                  Some("bar"),
                  Some("baz")
                )
              ),
              None,
              Seq.empty,
              Seq.empty,
              None,
              None
            )
          ),
          toClientDefinition(
            PublicDatasetDTO
              .apply(
                publicDataset1,
                publicDataset1_V2,
                Seq.empty,
                None,
                None,
                Seq.empty,
                Seq.empty,
                None,
                None
              )
          )
        ),
        totalCount = 2L
      )

      val response =
        datasetClient
          .getDatasets(
            tags = None,
            ids =
              Some(List(publicDataset1.id.toString, publicDataset2.id.toString))
          )
          .awaitFinite()
          .value

      assert(response === GetDatasetsResponse.OK(expected))
    }

    "fails call if strings in the list cannot be coerced to integers" in {
      // Dataset 1

      val publicDataset1 = TestUtilities.createDataset(ports.db)()
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.NotPublished
        )
      val publicDataset1_V2: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 2
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )

      ports.db.run(
        SponsorshipsMapper.createOrUpdate(
          publicDataset2.sourceOrganizationId,
          publicDataset2.sourceDatasetId,
          Some("foo"),
          Some("bar"),
          Some("baz")
        )
      )

      val expected = DatasetsPage(
        limit = 10,
        offset = 0,
        datasets = Vector(
          toClientDefinition(
            PublicDatasetDTO.apply(
              publicDataset2,
              publicDataset2_V1,
              IndexedSeq(),
              Some(
                Sponsorship(
                  publicDataset2.id,
                  Some("foo"),
                  Some("bar"),
                  Some("baz")
                )
              ),
              None,
              IndexedSeq(),
              IndexedSeq(),
              None,
              None
            )
          ),
          toClientDefinition(
            PublicDatasetDTO
              .apply(
                publicDataset1,
                publicDataset1_V2,
                IndexedSeq(),
                None,
                None,
                IndexedSeq(),
                IndexedSeq(),
                None,
                None
              )
          )
        ),
        totalCount = 2L
      )

      val response =
        datasetClient
          .getDatasets(
            tags = None,
            ids = Some(
              List(
                publicDataset1.id.toString,
                publicDataset2.id.toString,
                "plop"
              )
            )
          )
          .awaitFinite()
          .value

      assert(response === GetDatasetsResponse.BadRequest("ids must be numbers"))
    }

    "return the latest version of each published dataset filtered by tags" in {
      // Dataset 1
      val publicDataset1 =
        TestUtilities.createDataset(ports.db)(tags = List("abc", "red", "blue"))
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.NotPublished
        )
      val publicDataset1_V2: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        sourceOrganizationId = 1,
        sourceDatasetId = 2,
        tags = List("abc")
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )

      val response =
        datasetClient
          .getDatasets(tags = Some(List("abc", "red")))
          .awaitFinite()
          .value

      response shouldBe GetDatasetsResponse.OK(
        DatasetsPage(
          limit = 10,
          offset = 0,
          datasets = Vector(
            toClientDefinition(
              PublicDatasetDTO
                .apply(
                  publicDataset1,
                  publicDataset1_V2,
                  IndexedSeq.empty,
                  None,
                  None,
                  IndexedSeq.empty,
                  IndexedSeq.empty,
                  None,
                  None
                )
            )
          ),
          totalCount = 1L
        )
      )
    }

    "return 400 when query parameters are malformed" in {
      datasetClient
        .getDatasets(orderBy = Some("unknown-field"))
        .awaitFinite()
        .value shouldBe an[GetDatasetsResponse.BadRequest]

      datasetClient
        .getDatasets(orderDirection = Some("horizontally"))
        .awaitFinite()
        .value shouldBe an[GetDatasetsResponse.BadRequest]
    }
  }

  "GET /datasets/{id}/versions/{versionId}/download" should {

    behave like AccessIsRestrictedForEmbargoedData(
      (v: PublicDatasetVersion) => {

        // Must be mocked for endpoint to succeed
        ports.s3StreamClient
          .asInstanceOf[MockS3StreamClient]
          .withNextResponse(List.empty)

        Get(s"/datasets/${v.datasetId}/versions/${v.version}/download")
      }
    )

    "download a ZIP of the dataset" in withTempDirectory { tempDir: Path =>
      {

        val publicDataset = TestUtilities.createDataset(ports.db)()
        val publicDatasetV1 =
          TestUtilities.createNewDatasetVersion(ports.db)(
            id = publicDataset.id,
            status = PublishSucceeded
          )

        val testFiles = List(
          TestFile(1000, tempDir, "1.pdf", "test/1.pdf"),
          TestFile(72000, tempDir, "2.txt", "test/2.txt")
        )

        ports.s3StreamClient
          .asInstanceOf[MockS3StreamClient]
          .withNextResponse(testFiles)

        val outPath = tempDir.resolve("output.zip")
        implicit val timeout = RouteTestTimeout(5.seconds dilated)

        Get(
          s"/datasets/${publicDataset.id}/versions/${publicDatasetV1.version}/download"
        ) ~> createRoutes() ~> check {
          status shouldEqual StatusCodes.OK

          header("Content-Disposition") shouldEqual Some(
            new `Content-Disposition`(
              ContentDispositionTypes.attachment,
              Map(
                "filename" -> s"Pennsieve-dataset-${publicDataset.id}-version-${publicDatasetV1.version}.zip"
              )
            )
          )

          response.entity.dataBytes
            .runWith(FileIO.toPath(outPath))
            .awaitFinite()

          unzipArchive(outPath.toString, tempDir.toString)
          TestFile.sourceAndDestAreEqual(testFiles) shouldBe true
        }

        val updatedPublicDataset = ports.db
          .run(
            DatasetDownloadsMapper.getDatasetDownloadsByDatasetAndVersion(
              dataset = publicDataset,
              version = publicDatasetV1
            )
          )
          .await

        updatedPublicDataset.size shouldBe 1

      }
    }

    "not download a ZIP when the dataset is larger than the limit" in {

      val publicDataset = TestUtilities.createDataset(ports.db)()
      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded,
          size = Megabytes(520).toBytes.toLong // test limit is 512
        )

      Get(
        s"/datasets/${publicDataset.id}/versions/${publicDatasetV1.version}/download"
      ) ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.Forbidden
      }
    }

    "return 410 if the dataset has been unpublished" in {
      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.Unpublished
      )

      Get(
        s"/datasets/${publicDatasetV1.datasetId}/versions/${publicDatasetV1.version}/download"
      ) ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.Gone
      }
    }
  }

  "GET /datasets/{id}/versions/{versionId}/metadata" should {

    behave like AccessIsRestrictedForEmbargoedData(
      (v: PublicDatasetVersion) =>
        Get(s"/datasets/${v.datasetId}/versions/${v.version}/metadata")
    )

    "download manifest.json from S3 publish bucket" in {
      val publicDataset = TestUtilities.createDataset(ports.db)()
      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      addMetadata(ports.db, publicDatasetV1)

      Get(
        s"/datasets/${publicDataset.id}/versions/${publicDatasetV1.version}/metadata"
      ) ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.OK
        entityAs[String] shouldBe ports.s3StreamClient
          .asInstanceOf[MockS3StreamClient]
          .sampleMetadata
      }
    }

    "return 410 if the dataset has been unpublished" in {
      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.Unpublished
      )

      Get(
        s"/datasets/${publicDatasetV1.datasetId}/versions/${publicDatasetV1.version}/metadata"
      ) ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.Gone
      }
    }
  }

  "GET /datasets/{datasetId}/versions/{versionId}/files?path={path}" should {

    behave like AccessIsRestrictedForEmbargoedData((v: PublicDatasetVersion) => {

      // Extra setup needed for these request to succeed
      TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      Get(
        s"/datasets/${v.datasetId}/versions/${v.version}/files?path=A/file1.txt"
      )
    })

    "return the file with a full s3 path passed" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(
          v.datasetId,
          v.version,
          "s3://" + config.s3.publishBucket + f.s3Key.toString
        )
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.OK(
        client.definitions.File(
          name = f.name,
          path = "A/file1.txt",
          size = f.size,
          uri = "s3://" + config.s3.publishBucket + "/" + f.s3Key.toString,
          sourcePackageId = f.sourcePackageId,
          createdAt = Some(f.createdAt),
          fileType = FileType.Text,
          packageType = PackageType.Text,
          icon = utils.getIcon(FileType.Text),
          s3Version = Some(defaultS3VersionId)
        )
      )
    }
    "return the file with a s3 path without scheme and bucket" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(v.datasetId, v.version, f.s3Key.toString)
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.OK(
        client.definitions.File(
          name = f.name,
          path = "A/file1.txt",
          size = f.size,
          uri = "s3://" + config.s3.publishBucket + "/" + f.s3Key.toString,
          sourcePackageId = f.sourcePackageId,
          createdAt = Some(f.createdAt),
          fileType = FileType.Text,
          packageType = PackageType.Text,
          icon = utils.getIcon(FileType.Text),
          s3Version = Some(defaultS3VersionId)
        )
      )
    }

    "return the file with a s3 path without scheme and bucket and ignore the '/' begining a path" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(v.datasetId, v.version, "/" + f.s3Key.toString)
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.OK(
        client.definitions.File(
          name = f.name,
          path = "A/file1.txt",
          size = f.size,
          uri = "s3://" + config.s3.publishBucket + "/" + f.s3Key.toString,
          sourcePackageId = f.sourcePackageId,
          createdAt = Some(f.createdAt),
          fileType = FileType.Text,
          packageType = PackageType.Text,
          icon = utils.getIcon(FileType.Text),
          s3Version = Some(defaultS3VersionId)
        )
      )
    }

    "return the file with a s3 path without the datasetId and versionId parts" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(v.datasetId, v.version, "A/file1.txt")
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.OK(
        client.definitions.File(
          name = f.name,
          path = "A/file1.txt",
          size = f.size,
          uri = "s3://" + config.s3.publishBucket + "/" + f.s3Key.toString,
          sourcePackageId = f.sourcePackageId,
          createdAt = Some(f.createdAt),
          fileType = FileType.Text,
          packageType = PackageType.Text,
          icon = utils.getIcon(FileType.Text),
          s3Version = Some(defaultS3VersionId)
        )
      )
    }

    "fail to return the file with an incorrect s3 path" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(v.datasetId, v.version, "/" + f.s3Key.toString + "jhkafdsjkf")
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.NotFound(
        "/" + f.s3Key.toString + "jhkafdsjkf"
      )
    }

    "fail to return the file with a wrong dataset" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val wrongDatasetId = v.datasetId + 1
      val response = datasetClient
        .getFile(wrongDatasetId, v.version, f.s3Key.toString)
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.NotFound(wrongDatasetId.toString)
    }

    "fail to return the file with a wrong version" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val wrongVersion = v.version + 1
      val response = datasetClient
        .getFile(v.datasetId, wrongVersion, f.s3Key.toString)
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.NotFound(wrongVersion.toString)
    }

    "fail to return the file if the dataset is unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val v =
        TestUtilities.createNewDatasetVersion(ports.db)(
          dataset.id,
          status = PublishStatus.Unpublished
        )
      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = datasetClient
        .getFile(v.datasetId, v.version, f.s3Key.toString)
        .awaitFinite()
        .value

      response shouldBe GetFileResponse.Gone(
        client.definitions.TombstoneDto(
          id = dataset.id,
          version = v.version,
          name = dataset.name,
          tags = dataset.tags.toVector,
          status = PublishStatus.Unpublished,
          doi = v.doi,
          updatedAt = dataset.updatedAt
        )
      )
    }
  }

  "GET /datasets/{id}/versions/{versionId}/files/browse" should {

    behave like AccessIsRestrictedForEmbargoedData(
      (v: PublicDatasetVersion) =>
        Get(s"/datasets/${v.datasetId}/versions/${v.version}/files/browse")
    )

    "return toplevel files" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2"),
        s3Version = Some(defaultS3VersionId)
      )

      val response =
        datasetClient.browseFiles(v1.datasetId, v1.version).awaitFinite().value

      response shouldBe BrowseFilesResponse.OK(
        client.definitions.FileTreePage(
          totalCount = 2,
          limit = 100,
          offset = 0,
          files = Vector(
            client.definitions.Directory("A", "A", 200),
            client.definitions
              .File(
                "file3.txt",
                "file3.txt",
                100,
                FileType.Text,
                s"s3://bucket/${f3.s3Key}",
                PackageType.Text,
                Icon.Text,
                Some("N:package:2"),
                s3Version = Some(defaultS3VersionId)
              )
          )
        )
      )
    }

    "drop into a subdirectory" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2"),
        s3Version = Some(defaultS3VersionId)
      )

      val response =
        datasetClient
          .browseFiles(v1.datasetId, v1.version, Some("A"))
          .awaitFinite()
          .value

      response shouldBe BrowseFilesResponse.OK(
        client.definitions.FileTreePage(
          totalCount = 2,
          limit = 100,
          offset = 0,
          files = Vector(
            client.definitions
              .File(
                "file1.txt",
                "A/file1.txt",
                100,
                FileType.Text,
                s"s3://bucket/${f1.s3Key}",
                PackageType.Text,
                Icon.Text,
                Some("N:package:1"),
                s3Version = Some(defaultS3VersionId)
              ),
            client.definitions
              .File(
                "file2.txt",
                "A/file2.txt",
                100,
                FileType.Text,
                s"s3://bucket/${f2.s3Key}",
                PackageType.Text,
                Icon.Text,
                Some("N:package:1"),
                s3Version = Some(defaultS3VersionId)
              )
          )
        )
      )
    }
  }

  "GET /datasets/{id}/versions/{versionId}/files/download-manifest" should {

    behave like AccessIsRestrictedForEmbargoedData(
      (v: PublicDatasetVersion) =>
        Post(
          s"/datasets/${v.datasetId}/versions/${v.version}/files/download-manifest",
          HttpEntity.Strict(
            ContentTypes.`application/json`,
            ByteString(DownloadRequest(Vector("")).asJson.toString)
          )
        )
    )

    "return all files for a dataset" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2"),
        s3Version = Some(defaultS3VersionId)
      )
      val f4 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/B/file4.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some(defaultS3VersionId)
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector(""))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.OK(
        client.definitions.DownloadResponse(
          client.definitions
            .DownloadResponseHeader(4, f1.size + f2.size + f3.size + f4.size),
          Vector(
            client.definitions.DownloadResponseItem(
              f1.name,
              Vector("A"),
              s"https://bucket.s3.amazonaws.com/${f1.s3Key}",
              f1.size
            ),
            client.definitions.DownloadResponseItem(
              f2.name,
              Vector("A"),
              s"https://bucket.s3.amazonaws.com/${f2.s3Key}",
              f2.size
            ),
            client.definitions.DownloadResponseItem(
              f3.name,
              Vector.empty,
              s"https://bucket.s3.amazonaws.com/${f3.s3Key}",
              f3.size
            ),
            client.definitions.DownloadResponseItem(
              f4.name,
              Vector("A", "B"),
              s"https://bucket.s3.amazonaws.com/${f4.s3Key}",
              f4.size
            )
          )
        )
      )

      val updatedPublicDataset = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVersion(id = v1.datasetId, version = v1.version)
        )
        .await

      updatedPublicDataset.fileDownloadsCounter shouldBe 4

    }

    "return the children AND grandchildren of a subdirectory" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2")
      )
      val f4 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/B/file4.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector("A"))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.OK(
        client.definitions.DownloadResponse(
          client.definitions
            .DownloadResponseHeader(3, f1.size + f2.size + f4.size),
          Vector(
            client.definitions.DownloadResponseItem(
              f1.name,
              Vector("A"),
              s"https://bucket.s3.amazonaws.com/${f1.s3Key}",
              f1.size
            ),
            client.definitions.DownloadResponseItem(
              f2.name,
              Vector("A"),
              s"https://bucket.s3.amazonaws.com/${f2.s3Key}",
              f2.size
            ),
            client.definitions.DownloadResponseItem(
              f4.name,
              Vector("A", "B"),
              s"https://bucket.s3.amazonaws.com/${f4.s3Key}",
              f4.size
            )
          )
        )
      )
    }

    "return only a specified file" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2")
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector("file3.txt"))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.OK(
        client.definitions.DownloadResponse(
          client.definitions
            .DownloadResponseHeader(1, f3.size),
          Vector(
            client.definitions.DownloadResponseItem(
              f3.name,
              Vector.empty,
              s"https://bucket.s3.amazonaws.com/${f3.s3Key}",
              f3.size
            )
          )
        )
      )
    }

    "omit the root path from the returned paths if specified" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )
      val f3 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "file3.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:2")
      )
      val f4 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/B/file4.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1")
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector("A"), Some("A"))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.OK(
        client.definitions.DownloadResponse(
          client.definitions
            .DownloadResponseHeader(3, f1.size + f2.size + f4.size),
          Vector(
            client.definitions.DownloadResponseItem(
              f1.name,
              Vector.empty,
              s"https://bucket.s3.amazonaws.com/${f1.s3Key}",
              f1.size
            ),
            client.definitions.DownloadResponseItem(
              f2.name,
              Vector.empty,
              s"https://bucket.s3.amazonaws.com/${f2.s3Key}",
              f2.size
            ),
            client.definitions.DownloadResponseItem(
              f4.name,
              Vector("B"),
              s"https://bucket.s3.amazonaws.com/${f4.s3Key}",
              f4.size
            )
          )
        )
      )

      val nestedResponse =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector("A/B"), Some("A/B"))
          )
          .awaitFinite()
          .value

      nestedResponse shouldBe DownloadManifestResponse.OK(
        client.definitions.DownloadResponse(
          client.definitions
            .DownloadResponseHeader(1, f4.size),
          Vector(
            client.definitions.DownloadResponseItem(
              f4.name,
              Vector.empty,
              s"https://bucket.s3.amazonaws.com/${f4.s3Key}",
              f4.size
            )
          )
        )
      )
    }

    "return bad request if any path does not begin with the root path" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector("A", "B", "A/B"), Some("A/B"))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.BadRequest(
        "if root path is specified, all paths must begin with root path"
      )
    }

    "return forbidden if the total size is too large" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f1 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file1.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        size = Megabytes(260).toBytes.toLong // test limit is 512
      )
      val f2 = TestUtilities.createFileVersion(ports.db)(
        v1,
        "A/file2.txt",
        FileType.Text,
        sourcePackageId = Some("N:package:1"),
        size = Megabytes(260).toBytes.toLong // test limit is 512
      )

      val response =
        datasetClient
          .downloadManifest(
            v1.datasetId,
            v1.version,
            DownloadRequest(Vector(""))
          )
          .awaitFinite()
          .value

      response shouldBe DownloadManifestResponse.Forbidden(
        "requested files are too large to download"
      )
    }
  }

  "GET /dataset/{id}/data-use-agreement" should {
    "return latest agreement for a dataset" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      val dataset: PublicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDataset(publicDataset.id)
        )
        .await

      val response =
        datasetClient.getDataUseAgreement(publicDataset.id).awaitFinite().value

      response shouldBe GetDataUseAgreementResponse.OK(
        client.definitions
          .DataUseAgreementDto(
            id = 12,
            name = "Agreement #1",
            body = "Legal Text",
            organizationId = publicDataset.sourceOrganizationId
          )
      )
    }

    "fail if the dataset is not found" in {

      val response =
        datasetClient.downloadDataUseAgreement(5).awaitFinite().value

      response shouldBe DownloadDataUseAgreementResponse.NotFound("5")
    }

    "return 410 tombstone if the dataset has been unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          dataset.id,
          status = PublishStatus.Unpublished
        )

      val response = datasetClient
        .getDataUseAgreement(dataset.id)
        .awaitFinite()
        .value

      response shouldBe an[GetDataUseAgreementResponse.Gone]
    }
  }

  "GET /dataset/{id}/data-use-agreement/download" should {
    "download latest agreement for a dataset" in {

      val publicDataset =
        TestUtilities.createDataset(ports.db)()

      val publicDatasetV1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset.id,
          status = PublishSucceeded
        )

      Get(s"/datasets/${publicDataset.id}/data-use-agreement/download") ~> createRoutes() ~> check {
        status shouldEqual StatusCodes.OK

        header("Content-Disposition") shouldEqual Some(
          new `Content-Disposition`(
            ContentDispositionTypes.attachment,
            Map(
              "filename" -> s"Pennsieve-dataset-${publicDataset.id}-data-use-agreement.txt"
            )
          )
        )

        val body = response.entity
          .toStrict(5.seconds)
          .awaitFinite()

        body.data.utf8String shouldBe
          s"""${publicDataset.name} (${publicDatasetV1.doi})\n\n${LocalDate.now}\n\nLegal Text"""
      }
    }

    "fail if the dataset is not found" in {

      val response =
        datasetClient.downloadDataUseAgreement(5).awaitFinite().value

      response shouldBe DownloadDataUseAgreementResponse.NotFound("5")
    }

    "return 410 tombstone if the dataset has been unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          dataset.id,
          status = PublishStatus.Unpublished
        )

      val response = datasetClient
        .downloadDataUseAgreement(dataset.id)
        .awaitFinite()
        .value

      response shouldBe DownloadDataUseAgreementResponse.Gone
    }
  }

  "5.0 GET /datasets/{datasetId}/versions/{versionId}/files?path={path}" should {

    "return the file with a full s3 path passed" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded,
        migrated = true
      )

      val f = TestUtilities.createFileVersion(ports.db)(
        v,
        path = "A/file1.txt",
        fileType = FileType.Text,
        size = 1024,
        sourcePackageId = Some("N:package:1"),
        s3Version = Some("Version98765")
      )

      Get(
        s"/datasets/${v.datasetId}/versions/${v.version}/files?path=A/file1.txt"
      )

    }
  }

  "DatasetDTO response" should {
    "have default value for datasetType" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishSucceeded,
          doi = "10.12345/zyxw-jklm"
        )

      val response = datasetClient
        .getDatasetVersion(dataset.id, version.version)
        .awaitFinite()
        .value
        .asInstanceOf[GetDatasetVersionResponse.OK]
        .value

      response.datasetType shouldBe Some(DatasetType.Research.entryName)
    }

    "have datasetType 'release' for Code Repos" in {
      val dataset =
        TestUtilities.createDataset(ports.db)(datasetType = DatasetType.Release)
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishSucceeded,
          doi = "10.12345/zyxw-jklm"
        )

      val response = datasetClient
        .getDatasetVersion(dataset.id, version.version)
        .awaitFinite()
        .value
        .asInstanceOf[GetDatasetVersionResponse.OK]
        .value

      response.datasetType shouldBe Some(DatasetType.Release.entryName)

      val json = response.asJson.toString()
    }

    "have release info for Code Repo" in {
      val dataset = TestUtilities.createDataset(ports.db)(
        name = "dataset-dto test for releaes",
        datasetType = DatasetType.Release,
        sourceOrganizationId = 1,
        sourceDatasetId = 901
      )
      val version = TestUtilities.createNewDatasetVersion(ports.db)(
        id = dataset.id,
        status = PublishStatus.PublishSucceeded
      )
      val release = TestUtilities.createDatasetRelease(ports.db)(
        datasetId = dataset.id,
        versionId = version.version,
        origin = "GitHub",
        label = "v1.0.0",
        repoUrl = "https://github.com/Pennsieve/test-repo"
      )

      val response = datasetClient
        .getDatasetVersion(dataset.id, version.version)
        .awaitFinite()
        .value
        .asInstanceOf[GetDatasetVersionResponse.OK]
        .value

      response.release.isDefined shouldBe true
    }

  }

  "dataset type filtering" should {
    "return everything when not specified" in {
      // Dataset 1
      val publicDataset1 = TestUtilities.createDataset(ports.db)(
        name = "unfiltered dataset 1",
        datasetType = DatasetType.Research,
        sourceOrganizationId = 1,
        sourceDatasetId = 101
      )
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor1 = TestUtilities.createContributor(ports.db)(
        firstName = "Alberto",
        lastName = "Ascari",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = 1,
        version = publicDataset1_V1.version,
        sourceContributorId = 51,
        sourceUserId = Some(51)
      )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        name = "unfiltered dataset 2",
        datasetType = DatasetType.Collection,
        sourceOrganizationId = 1,
        sourceDatasetId = 102
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor2 = TestUtilities.createContributor(ports.db)(
        firstName = "Tony",
        lastName = "Bettenhausen",
        orcid = None,
        datasetId = publicDataset2.id,
        organizationId = 1,
        version = publicDataset2_V1.version,
        sourceContributorId = 52,
        sourceUserId = Some(52)
      )

      // Dataset 3
      val publicDataset3 = TestUtilities.createDataset(ports.db)(
        name = "unfiltered dataset 3",
        datasetType = DatasetType.Release,
        sourceOrganizationId = 1,
        sourceDatasetId = 103
      )
      val publicDataset3_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset3.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor3 = TestUtilities.createContributor(ports.db)(
        firstName = "Prince",
        lastName = "Bira",
        orcid = None,
        datasetId = publicDataset3.id,
        organizationId = 1,
        version = publicDataset3_V1.version,
        sourceContributorId = 53,
        sourceUserId = Some(53)
      )

      val allDatasets = IndexedSeq(
        toClientDefinition(
          PublicDatasetDTO.apply(
            publicDataset3,
            publicDataset3_V1,
            IndexedSeq(PublicContributorDTO.apply(contributor3)),
            None,
            None,
            Some(IndexedSeq.empty),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        ),
        toClientDefinition(
          PublicDatasetDTO.apply(
            publicDataset2,
            publicDataset2_V1,
            IndexedSeq(PublicContributorDTO.apply(contributor2)),
            None,
            None,
            Some(IndexedSeq.empty),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        ),
        toClientDefinition(
          PublicDatasetDTO(
            publicDataset1,
            publicDataset1_V1,
            IndexedSeq(PublicContributorDTO.apply(contributor1)),
            None,
            None,
            Some(IndexedSeq.empty),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        )
      )

      assert(
        datasetClient
          .getDatasets()
          .awaitFinite()
          .value === GetDatasetsResponse.OK(
          DatasetsPage(
            limit = 10,
            offset = 0,
            datasets = allDatasets.toVector,
            totalCount = allDatasets.size.toLong
          )
        )
      )
    }

    "return only requested types when specified" in {
      // Dataset 1
      val publicDataset1 = TestUtilities.createDataset(ports.db)(
        name = "filtered dataset 1",
        datasetType = DatasetType.Research,
        sourceOrganizationId = 1,
        sourceDatasetId = 201
      )
      val publicDataset1_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor1 = TestUtilities.createContributor(ports.db)(
        firstName = "Richard",
        lastName = "Petty",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = 1,
        version = publicDataset1_V1.version,
        sourceContributorId = 61,
        sourceUserId = Some(61)
      )

      // Dataset 2
      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        name = "filtered dataset 2",
        datasetType = DatasetType.Collection,
        sourceOrganizationId = 1,
        sourceDatasetId = 202
      )
      val publicDataset2_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor2 = TestUtilities.createContributor(ports.db)(
        firstName = "Junior",
        lastName = "Johnson",
        orcid = None,
        datasetId = publicDataset2.id,
        organizationId = 1,
        version = publicDataset2_V1.version,
        sourceContributorId = 62,
        sourceUserId = Some(62)
      )

      // Dataset 3
      val publicDataset3 = TestUtilities.createDataset(ports.db)(
        name = "filtered dataset 3",
        datasetType = DatasetType.Release,
        sourceOrganizationId = 1,
        sourceDatasetId = 203
      )
      val publicDataset3_V1: PublicDatasetVersion =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset3.id,
          status = PublishStatus.PublishSucceeded
        )
      val contributor3 = TestUtilities.createContributor(ports.db)(
        firstName = "Ned",
        lastName = "Jarrett",
        orcid = None,
        datasetId = publicDataset3.id,
        organizationId = 1,
        version = publicDataset3_V1.version,
        sourceContributorId = 63,
        sourceUserId = Some(63)
      )

      val filteredDatasets = IndexedSeq(
        toClientDefinition(
          PublicDatasetDTO.apply(
            publicDataset3,
            publicDataset3_V1,
            IndexedSeq(PublicContributorDTO.apply(contributor3)),
            None,
            None,
            Some(IndexedSeq.empty),
            Some(IndexedSeq.empty),
            None,
            None,
            None
          )
        )
      )

      assert(
        datasetClient
          .getDatasets(datasetType = Some("release"))
          .awaitFinite()
          .value === GetDatasetsResponse.OK(
          DatasetsPage(
            limit = 10,
            offset = 0,
            datasets = filteredDatasets.toVector,
            totalCount = filteredDatasets.size.toLong
          )
        )
      )
    }
  }

  def setupForReleaseAssetTesting(
  ): (PublicDataset, PublicDatasetVersion, ReleaseAssetListing) = {
    // create dataset
    val dataset = TestUtilities.createDataset(ports.db)(
      name = "filtered dataset 1",
      datasetType = DatasetType.Release,
      sourceOrganizationId = 1,
      sourceDatasetId = 333
    )

    // create version
    val version: PublicDatasetVersion =
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = dataset.id,
        status = PublishStatus.PublishSucceeded
      )
    // create release
    val release: PublicDatasetRelease =
      TestUtilities.createDatasetRelease(ports.db)(
        dataset.id,
        version.version,
        "GitHub",
        "v1.2.3",
        "https://github.com/pennsieve/test-browse-repo",
        "1a2b3c4d5e6f7g8h9i"
      )

    // create assets
    val listing = ReleaseAssetListing(
      files = List(
        ReleaseAssetFile(
          file = "LICENSE",
          name = "LICENSE",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "README.md",
          name = "README.md",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "CHANGELOG.md",
          name = "CHANGELOG.md",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/",
          name = "code",
          `type` = ReleaseAssetFileType.Folder,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/main.py",
          name = "main.py",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/utils.py",
          name = "utils.py",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/reporting.py",
          name = "reporting.py",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/testing/",
          name = "testing",
          `type` = ReleaseAssetFileType.Folder,
          size = 0
        ),
        ReleaseAssetFile(
          file = "code/testing/run-tests.py",
          name = "run-tests.py",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "data/",
          name = "data",
          `type` = ReleaseAssetFileType.Folder,
          size = 0
        ),
        ReleaseAssetFile(
          file = "data/subjects.csv",
          name = "subjects.csv",
          `type` = ReleaseAssetFileType.File,
          size = 0
        ),
        ReleaseAssetFile(
          file = "data/metadata.dat",
          name = "metadata.dat",
          `type` = ReleaseAssetFileType.File,
          size = 0
        )
      )
    )
    val _ =
      TestUtilities.addReleaseAssetFiles(ports.db)(version, release, listing)

    (dataset, version, listing)
  }

  "release asset browse" should {
    "return all assets" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAllAssets(dataset.id, version.version)
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAllAssetsResponse.OK]
        .value

      response.assets.length shouldEqual listing.files.length
    }

    "return top-level assets" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAssets(dataset.id, version.version, path = None)
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAssetsResponse.OK]
        .value

      response.assets.length shouldEqual 5
    }

    "return only selected assets" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAssets(dataset.id, version.version, path = Some("code"))
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAssetsResponse.OK]
        .value

      response.assets.length shouldEqual 4
    }

    "return assets from nested folder" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAssets(dataset.id, version.version, path = Some("code/testing"))
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAssetsResponse.OK]
        .value

      response.assets.length shouldEqual 1
    }

    "return nothing when path does not exist" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAssets(dataset.id, version.version, path = Some("nothing"))
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAssetsResponse.OK]
        .value

      response.assets.length shouldEqual 0
    }

    "not throw Internal Server Error when path is the empty string" in {
      val (dataset, version, listing) = setupForReleaseAssetTesting()

      val response = datasetClient
        .browseAssets(dataset.id, version.version, path = Some(""))
        .awaitFinite()
        .value
        .asInstanceOf[BrowseAssetsResponse.OK]
        .value

      response.assets.length shouldEqual 5
    }
  }
}
