// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.generateServiceToken
import com.pennsieve.discover.ServiceSpecHarness
import com.pennsieve.discover.client.definitions
import com.pennsieve.discover.client.definitions.{
  DatasetPublishStatus,
  InternalContributor,
  ReleasePublishingResponse
}
import com.pennsieve.discover.client.publish.{ PublishClient, PublishResponse }
import com.pennsieve.discover.client.release.{
  FinalizeReleaseResponse,
  PublishReleaseResponse,
  ReleaseClient
}
import com.pennsieve.discover.clients.{
  MockDoiClient,
  MockLambdaClient,
  MockS3StreamClient
}
import com.pennsieve.discover.db.{
  PublicDatasetReleaseAssetMapper,
  PublicDatasetReleaseMapper,
  PublicDatasetVersionFilesTableMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFileVersionsMapper
}
import com.pennsieve.discover.models.{ PublishingWorkflow, S3CleanupStage }
import com.pennsieve.discover.client.definitions.BucketConfig
import com.pennsieve.discover.server.definitions.FinalizeReleaseRequest
import com.pennsieve.models.{ Degree, License, PublishStatus, RelationshipType }
import com.pennsieve.models.PublishStatus.PublishInProgress
import com.pennsieve.test.EitherValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.parser.decode

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class ReleaseHandlerSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(ReleaseHandler.routes(ports))

  def createClient(routes: Route): ReleaseClient =
    ReleaseClient.httpClient(Route.toFunction(routes))

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

  val origin = "GitHub"
  val repoUrl = "https://github.com/Pennsieve/test-repo"
  val label = "v1.0.0"
  val marker = "4b6083761af527c197ee1549c77b30857dec9542"

  val internalContributor =
    new InternalContributor(
      1,
      "Sally",
      "Field",
      middleInitial = Some("M"),
      degree = Some(Degree.BS)
    )

  val internalExternalPublication =
    new definitions.InternalExternalPublication(
      doi = "10.26275/t6j6-77pu",
      relationshipType = Some(RelationshipType.Describes)
    )

  val client = createClient(createRoutes())
  val token: Jwt.Token =
    generateServiceToken(
      ports.jwt,
      organizationId = organizationId,
      datasetId = datasetId
    )

  val authToken = List(Authorization(OAuth2BearerToken(token.value)))

  def releaseRequest(
    origin: String,
    label: String,
    marker: String,
    repoUrl: String,
    labelUrl: Option[String] = None,
    markerUrl: Option[String] = None,
    releaseStatus: Option[String] = None
  ): definitions.PublishReleaseRequest = {
    definitions.PublishReleaseRequest(
      name = datasetName,
      description = "A very very long description...",
      ownerId = 1,
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
      origin = origin,
      label = label,
      marker = marker,
      repoUrl = repoUrl,
      labelUrl = labelUrl,
      markerUrl = markerUrl,
      releaseStatus = releaseStatus
    )
  }

  "POST /organizations/{organizationId}/datasets/{datasetId}/release" should {
    "create a dataset, version and release" in {
      val requestBody = releaseRequest(origin, label, marker, repoUrl)
      val response = client
        .publishRelease(organizationId, datasetId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishReleaseResponse.Created]
        .value

      response.name shouldBe requestBody.name
      response.sourceDatasetId shouldBe datasetId
//        name = ???,
//        sourceOrganizationName = ???,
//        sourceOrganizationId = ???,
//        sourceDatasetId = ???,
//        publishedDatasetId = ???,
//        publishedVersionCount = ???,
//        status = ???,
//        lastPublishedDate = ???,
//        sponsorship = None,
//        publicId = ???
//      )

//      response shouldBe ReleasePublishingResponse(
//        datasetName,
//        organizationId,
//        datasetId,
//        None,
//        0,
//        PublishInProgress,
//        None,
//      )

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

      val doiDto = ports.doiClient
        .asInstanceOf[MockDoiClient]
        .getMockDoi(organizationId, datasetId)
        .get

      doiDto.publisher shouldBe "Pennsieve Discover"

      val publicRelease = ports.db
        .run(
          PublicDatasetReleaseMapper
            .get(publicDataset.id, publicVersion.version)
        )
        .awaitFinite()
        .get

      publicRelease.origin shouldBe origin
      publicRelease.label shouldBe label
      publicRelease.marker shouldBe marker
      publicRelease.repoUrl shouldBe repoUrl
    }
  }

  "ReleaseHandler" should {
    "publish a Code Repo" in {
      // step 1: initialize publication
      val requestBody = releaseRequest(origin, label, marker, repoUrl)
      val publishReleaseResponse = client
        .publishRelease(organizationId, datasetId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishReleaseResponse.Created]
        .value

      publishReleaseResponse.name shouldBe requestBody.name
      publishReleaseResponse.sourceDatasetId shouldBe datasetId

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

      // generate manifest.json
      val metadataJson =
        s"""
           |{
           |    "datePublished": "2024-11-04",
           |    "pennsieveDatasetId": 5169,
           |    "version": 1,
           |    "name": "muftring/test-github-publishing-manual",
           |    "description": "a test repo to test the manual publishing of this code repo",
           |    "creator": {
           |        "id": 0,
           |        "first_name": "Michael",
           |        "last_name": "Uftring",
           |        "degree": "M.S.",
           |        "orcid": "0000-0001-7054-4685"
           |    },
           |    "contributors": [
           |        {
           |            "id": 0,
           |            "first_name": "Michael",
           |            "last_name": "Uftring"
           |        }
           |    ],
           |    "sourceOrganization": "Publishing 5.0 Workspace",
           |    "keywords": [],
           |    "license": "MIT License",
           |    "@id": "10.21397/u4cj-xqpt",
           |    "publisher": "The University of Pennsylvania",
           |    "@context": "http://schema.org/",
           |    "@type": "Release",
           |    "schemaVersion": "http://schema.org/version/3.7/",
           |    "files": [
           |        {
           |            "name": "changelog.md",
           |            "path": "changelog.md",
           |            "size": 122,
           |            "fileType": "Markdown",
           |            "s3VersionId": "lQajWqKYKuA76Ax7Jj5osiIWJP9E1DMJ"
           |        },
           |        {
           |            "name": "readme.md",
           |            "path": "readme.md",
           |            "size": 286,
           |            "fileType": "Markdown",
           |            "s3VersionId": "pQ4CLbmcpOOXKDb6VNfKvuldpzs_b0wH"
           |        },
           |        {
           |            "name": "muftring-test-github-publishing-manual-v1.0.3-0-g3931565.zip",
           |            "path": "assets/muftring-test-github-publishing-manual-v1.0.3-0-g3931565.zip",
           |            "size": 8586,
           |            "fileType": "ZIP",
           |            "s3VersionId": "qWBDnQf47ZTYB97Tal7vIeSJpSqYMQiq"
           |        },
           |        {
           |            "name": "manifest.json",
           |            "path": "manifest.json",
           |            "size": 2010,
           |            "fileType": "Json"
           |        }
           |    ],
           |    "release": {
           |        "origin": "GitHub",
           |        "url": "https://github.com/muftring/test-github-publishing-manual",
           |        "label": "v1.0.3",
           |        "marker": "3931565f392628e48c4158f8262a6e728207cb84"
           |    },
           |    "pennsieveSchemaVersion": "5.0"
           |}
           |""".stripMargin
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .storeDatasetMetadata(publicVersion, metadataJson)

      // generate release-asset-listing.json
      val releaseAssetListingJson =
        s"""
           |{
           |  "files": [
           |    {
           |      "file": "LICENSE",
           |      "name": "LICENSE",
           |      "type": "file",
           |      "size": 1072
           |    },
           |    {
           |      "file": "README.md",
           |      "name": "README.md",
           |      "type": "file",
           |      "size": 286
           |    },
           |    {
           |      "file": "code/",
           |      "name": "code",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "code/graph.py",
           |      "name": "graph.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/main.py",
           |      "name": "main.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/package.py",
           |      "name": "package.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/reporting.py",
           |      "name": "reporting.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/testing.py",
           |      "name": "testing.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "data/",
           |      "name": "data",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "data/patient.dat",
           |      "name": "patient.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/sample.dat",
           |      "name": "sample.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/study.dat",
           |      "name": "study.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/visit.dat",
           |      "name": "visit.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "model/",
           |      "name": "model",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "model/metadata.json",
           |      "name": "metadata.json",
           |      "type": "file",
           |      "size": 582
           |    }
           |  ]
           |}
           |""".stripMargin
      ports.s3StreamClient
        .asInstanceOf[MockS3StreamClient]
        .storeReleaseAssetListing(publicVersion, releaseAssetListingJson)

      // step 2: finalize publication
      val finalizeRequest =
        com.pennsieve.discover.client.definitions.FinalizeReleaseRequest(
          publishId = publicVersion.datasetId,
          versionId = publicVersion.version,
          publishSuccess = true,
          fileCount = 4,
          totalSize = 1234,
          manifestKey =
            s"${publicVersion.datasetId}/${publicVersion.version}/manifest.json",
          manifestVersionId = "c56773cdf2ea4f45b24b98f4a3580bdc",
          bannerKey = None,
          changelogKey = Some(
            s"${publicVersion.datasetId}/${publicVersion.version}/changelog.md"
          ),
          readmeKey = Some(
            s"${publicVersion.datasetId}/${publicVersion.version}/readme.md"
          )
        )
      val finalizeReleaseResponse = client
        .finalizeRelease(organizationId, datasetId, finalizeRequest, authToken)
        .awaitFinite(Duration(30, TimeUnit.SECONDS))
        .value
        .asInstanceOf[FinalizeReleaseResponse.OK]
        .value

      // check: dataset, version, release, assets, files
      val publicDatasetFinal = ports.db
        .run(
          PublicDatasetsMapper
            .getDatasetFromSourceIds(organizationId, datasetId)
        )
        .awaitFinite()

      val publicVersionFinal = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDatasetFinal.id)
        )
        .awaitFinite()
        .get

      val links = ports.db
        .run(
          PublicDatasetVersionFilesTableMapper
            .getLinks(publicDatasetFinal.id, publicVersionFinal.version)
        )
        .awaitFinite()

      val files = ports.db
        .run(PublicFileVersionsMapper.getAll(publicDatasetFinal.id))
        .awaitFinite()

      val assets = ports.db
        .run(
          PublicDatasetReleaseAssetMapper
            .get(publicDatasetFinal, publicVersionFinal)
        )
        .awaitFinite()

      publicVersionFinal.status shouldBe PublishStatus.PublishSucceeded
      files.length shouldEqual 4
      links.length shouldEqual 4
      assets.length shouldEqual 15
    }
  }

  "JSON encoder/decoder" should {
    "decode PublishReleaseRequest message" in {
      val jsonMessage =
        """
          |{
          |  "name": "muftring/a1a-test-repo",
          |  "description": "Test Repo a1a for Publishing",
          |  "ownerId": 177,
          |  "fileCount": 0,
          |  "size": 0,
          |  "license": "Apache License 2.0",
          |  "contributors": [
          |    {
          |      "id": 1,
          |      "firstName": "Michael",
          |      "lastName": "Uftring",
          |      "orcid": "0000-0001-7054-4685",
          |      "userId": 177
          |    }
          |  ],
          |  "tags": [],
          |  "origin": "GitHub",
          |  "label": "v1.5.9",
          |  "marker": "ad6919eb3391ad24f7d73f04a07dd088e153373e",
          |  "repoUrl": "https://github.com/muftring/a1a-test-repo",
          |  "ownerNodeId": "N:user:61e7c1cf-a836-421b-b919-a2309402c9d6",
          |  "ownerFirstName": "Michael",
          |  "ownerLastName": "Uftring",
          |  "ownerOrcid": "0000-0001-7054-4685",
          |  "organizationNodeId": "N:organization:7c2de0a6-5972-4138-99ad-cc0aff0fb67f",
          |  "organizationName": "Publishing 5.0 Workspace",
          |  "datasetNodeId": "N:dataset:749c25b7-0d42-4cfa-b7ed-996fed1314c6",
          |  "bucketConfig": {
          |    "publish": "pennsieve-dev-discover-publish50-use1",
          |    "embargo": "pennsieve-dev-discover-publish50-use1"
          |  }
          |}
          |
          |""".stripMargin

      decode[definitions.PublishReleaseRequest](jsonMessage) shouldBe Right(
        definitions.PublishReleaseRequest(
          name = "muftring/a1a-test-repo",
          description = "Test Repo a1a for Publishing",
          ownerId = 177,
          fileCount = 0,
          size = 0,
          license = License.`Apache License 2.0`,
          contributors = Vector(
            InternalContributor(
              id = 1,
              firstName = "Michael",
              lastName = "Uftring",
              orcid = Some("0000-0001-7054-4685"),
              middleInitial = None,
              degree = None,
              userId = Some(177)
            )
          ),
          collections = None,
          externalPublications = None,
          tags = Vector.empty,
          ownerNodeId = "N:user:61e7c1cf-a836-421b-b919-a2309402c9d6",
          ownerFirstName = "Michael",
          ownerLastName = "Uftring",
          ownerOrcid = "0000-0001-7054-4685",
          organizationNodeId =
            "N:organization:7c2de0a6-5972-4138-99ad-cc0aff0fb67f",
          organizationName = "Publishing 5.0 Workspace",
          datasetNodeId = "N:dataset:749c25b7-0d42-4cfa-b7ed-996fed1314c6",
          bucketConfig = Some(
            BucketConfig(
              publish = "pennsieve-dev-discover-publish50-use1",
              embargo = "pennsieve-dev-discover-publish50-use1"
            )
          ),
          origin = "GitHub",
          label = "v1.5.9",
          marker = "ad6919eb3391ad24f7d73f04a07dd088e153373e",
          repoUrl = "https://github.com/muftring/a1a-test-repo",
          labelUrl = None,
          markerUrl = None,
          releaseStatus = None
        )
      )
    }
  }

  "Finalize" should {
    "handle Failure notifications" in {
      // step 1: initialize publication
      val requestBody = releaseRequest(origin, label, marker, repoUrl)
      val publishReleaseResponse = client
        .publishRelease(organizationId, datasetId, requestBody, authToken)
        .awaitFinite()
        .value
        .asInstanceOf[PublishReleaseResponse.Created]
        .value

      publishReleaseResponse.name shouldBe requestBody.name
      publishReleaseResponse.sourceDatasetId shouldBe datasetId

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

      publicVersion.status shouldBe PublishStatus.PublishInProgress

      // step 2. notify of failure
      val finalizeRequest =
        com.pennsieve.discover.client.definitions.FinalizeReleaseRequest(
          publishId = publicVersion.datasetId,
          versionId = publicVersion.version,
          publishSuccess = false,
          fileCount = 0,
          totalSize = 0,
          manifestKey = "",
          manifestVersionId = "",
          bannerKey = None,
          changelogKey = None,
          readmeKey = None
        )
      val finalizeReleaseResponse = client
        .finalizeRelease(organizationId, datasetId, finalizeRequest, authToken)
        .awaitFinite(Duration(30, TimeUnit.SECONDS))
        .value
        .asInstanceOf[FinalizeReleaseResponse.OK]
        .value

      finalizeReleaseResponse.status shouldBe PublishStatus.PublishFailed

      val updatedVersion = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDataset.id)
        )
        .awaitFinite()
        .get

      updatedVersion.status shouldBe PublishStatus.PublishFailed

      // check that S3 Clean was invoked with action = "failure"
      val s3CleanRequests =
        ports.lambdaClient.asInstanceOf[MockLambdaClient].requests
      s3CleanRequests.length shouldEqual 1
      s3CleanRequests.head.cleanupStage shouldBe S3CleanupStage.Failure
    }
  }

}
