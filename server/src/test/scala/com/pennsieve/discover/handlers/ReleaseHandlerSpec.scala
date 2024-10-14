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
  InternalContributor
}
import com.pennsieve.discover.client.publish.{ PublishClient, PublishResponse }
import com.pennsieve.discover.client.release.{
  PublishReleaseResponse,
  ReleaseClient
}
import com.pennsieve.discover.clients.MockDoiClient
import com.pennsieve.discover.db.{
  PublicDatasetReleaseMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper
}
import com.pennsieve.discover.models.PublishingWorkflow
import com.pennsieve.discover.client.definitions.BucketConfig
import com.pennsieve.models.{ Degree, License, RelationshipType }
import com.pennsieve.models.PublishStatus.PublishInProgress
import com.pennsieve.test.EitherValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.parser.decode

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

      response shouldBe DatasetPublishStatus(
        datasetName,
        organizationId,
        datasetId,
        None,
        0,
        PublishInProgress,
        None,
        workflowId = PublishingWorkflow.Version5
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

  "Auth Middleware" should {
    "parse Service CLaim" in {
      val serviceClaim = ""

    }
  }

}
