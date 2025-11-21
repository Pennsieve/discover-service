// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.generateUserToken
import com.pennsieve.discover.client.definitions.{
  DatasetMetrics,
  DatasetMetricsDto
}
import com.pennsieve.discover.client.organization.{
  GetOrganizationDatasetMetricsResponse,
  OrganizationClient
}
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.models.PublishStatus
import com.pennsieve.test.EitherValue._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OrganizationHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  val organizationId = 1

  def createRoutes(): Route =
    Route.seal(OrganizationHandler.routes(ports))

  def createClient(routes: Route): OrganizationClient =
    OrganizationClient.httpClient(Route.toFunction(routes))

  var organizationClient: OrganizationClient = _

  override def afterStart(): Unit = {
    super.afterStart()
    organizationClient = createClient(createRoutes())
  }

  "GET /organizations/{organizationId}/datasets/metrics" should {

    "return dataset metrics for a given organization" in {
      val sourceIdDatasetVersions = (1 to 3).map { i =>
        {
          val dataset = TestUtilities.createDatasetV1(ports.db)(
            sourceOrganizationId = organizationId,
            sourceDatasetId = i,
            name = s"Dataset ${i}",
            status = PublishStatus.PublishSucceeded
          )
          (i, dataset)
        }
      }

      val datasetIdMap = sourceIdDatasetVersions.map {
        case (sourceId, version) => sourceId -> version.datasetId
      }.toMap

      sourceIdDatasetVersions.foreach {
        case (i, dataset) => {
          (1 to 3).foreach { j =>
            {
              TestUtilities.createNewDatasetVersion(ports.db)(
                id = dataset.datasetId,
                size = i * j * 1000,
                status = PublishStatus.PublishSucceeded
              )
            }
          }
        }
      }

      val response =
        organizationClient
          .getOrganizationDatasetMetrics(organizationId)
          .awaitFinite()
          .value

      val expected = Vector(
        DatasetMetrics(datasetIdMap(3), "Dataset 3", 9000),
        DatasetMetrics(datasetIdMap(2), "Dataset 2", 6000),
        DatasetMetrics(datasetIdMap(1), "Dataset 1", 3000)
      )

      response shouldBe GetOrganizationDatasetMetricsResponse.OK(
        DatasetMetricsDto(expected)
      )
    }
  }
}
