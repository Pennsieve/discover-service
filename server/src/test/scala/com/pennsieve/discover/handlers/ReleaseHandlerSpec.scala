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
import com.pennsieve.discover.models.PublishingWorkflow
import com.pennsieve.models.{ Degree, License, RelationshipType }
import com.pennsieve.models.PublishStatus.PublishInProgress
import com.pennsieve.test.EitherValue._
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

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
      workflowId = Some(5),
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
    }
  }

}
