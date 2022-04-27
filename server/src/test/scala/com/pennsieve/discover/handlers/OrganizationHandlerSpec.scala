// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.auth.middleware.Jwt
import com.pennsieve.discover.Authenticator.generateUserToken
import com.pennsieve.discover.client.definitions.{
  DatasetMetrics,
  DatasetMetricsDTO
}
import com.pennsieve.discover.client.organization.{
  GetOrganizationDatasetMetricsResponse,
  OrganizationClient
}
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.models.PublishStatus
import com.pennsieve.test.EitherValue._
import org.scalatest.{ Matchers, WordSpec }

class OrganizationHandlerSpec
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  val organizationId = 1

  def createRoutes(): Route =
    Route.seal(OrganizationHandler.routes(ports))

  def createClient(routes: Route): OrganizationClient =
    OrganizationClient.httpClient(Route.asyncHandler(routes))

  val organizationClient: OrganizationClient = createClient(createRoutes())

  "GET /organizations/{organizationId}/datasets/metrics" should {

    "return dataset metrics for a given organization" in {
      val datasets = (1 to 3).map { i =>
        {
          TestUtilities.createDatasetV1(ports.db)(
            sourceOrganizationId = organizationId,
            sourceDatasetId = i,
            name = s"Dataset ${i}",
            status = PublishStatus.PublishSucceeded
          )
        }
      }

      datasets.foldLeft(1) {
        case (i, dataset) => {
          (1 to 3).map { j =>
            {
              TestUtilities.createNewDatasetVersion(ports.db)(
                id = dataset.datasetId,
                size = i * j * 1000,
                status = PublishStatus.PublishSucceeded
              )
            }
          }
          i + 1
        }
      }

      val response =
        organizationClient
          .getOrganizationDatasetMetrics(organizationId)
          .awaitFinite()
          .value

      val expected = IndexedSeq(
        DatasetMetrics(3, "Dataset 3", 9000),
        DatasetMetrics(2, "Dataset 2", 6000),
        DatasetMetrics(1, "Dataset 1", 3000)
      )

      response shouldBe GetOrganizationDatasetMetricsResponse.OK(
        DatasetMetricsDTO(expected)
      )
    }
  }
}
