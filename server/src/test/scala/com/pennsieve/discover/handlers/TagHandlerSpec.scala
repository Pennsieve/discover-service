// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.TestUtilities.TempDirectoryFixture
import com.pennsieve.discover.client.definitions.DatasetTag
import com.pennsieve.discover.client.tag.{ GetTagsResponse, TagClient }
import com.pennsieve.models.PublishStatus
import com.pennsieve.test.EitherValue._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TagHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness
    with TempDirectoryFixture {

  def createRoutes(): Route =
    Route.seal(TagHandler.routes(ports))

  def createClient(routes: Route): TagClient =
    TagClient.httpClient(Route.toFunction(routes))

  val tagClient: TagClient = createClient(createRoutes())

  "GET /tags" should {

    "return a list of dataset tags with counts" in {
      val datasets = (1 to 3).map { i =>
        TestUtilities.createDatasetV1(ports.db)(
          sourceDatasetId = i,
          tags = List("tag1", "tag2", "tag3"),
          status = PublishStatus.PublishSucceeded
        )
      }

      datasets.foreach { dataset =>
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.datasetId,
          status = PublishStatus.PublishSucceeded
        )
      }

      for (i <- 4 to 5) {
        TestUtilities.createDatasetV1(ports.db)(
          sourceDatasetId = i,
          tags = List("Tag1", "tag4", "tag5 "),
          status = PublishStatus.PublishSucceeded
        )
      }

      val expected = Vector(
        DatasetTag("tag1", 5),
        DatasetTag("tag2", 3),
        DatasetTag("tag3", 3),
        DatasetTag("tag4", 2),
        DatasetTag("tag5", 2)
      )

      val response =
        tagClient.getTags().awaitFinite().value

      response shouldBe GetTagsResponse.OK(expected)
    }

  }

}
