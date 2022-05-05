// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover.client.AkkaHttpImplicits._
import com.pennsieve.discover.server.definitions.{ File, FileTreeWithOrgPage }
import com.pennsieve.models.{ FileType, PackageType, PublishStatus }
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContextExecutor

class ServerRoutesSpec
    extends AnyWordSpec
    with Matchers
    with Inspectors
    with ScalatestRouteTest
    with ServiceSpecHarness {

  override implicit val executor: ExecutionContextExecutor =
    super[ScalatestRouteTest].executor

  val serverRoutes = Route.seal(Server.createRoutes(ports))

  "GET /packages/packageId/files" should {

    def createPackage(expectedPackageId: String): FileTreeWithOrgPage = {
      val expectedOrgId = 3

      val v1 = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = expectedOrgId,
        status = PublishStatus.PublishSucceeded
      )

      val f1 = TestUtilities.createFile(ports.db)(
        v1,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some(expectedPackageId)
      )

      FileTreeWithOrgPage(
        totalCount = 1,
        limit = 100,
        offset = 0,
        organizationId = expectedOrgId,
        files = Vector(
          File(
            name = f1.name,
            path = "A/file1.txt",
            size = f1.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f1.s3Key.toString,
            sourcePackageId = f1.sourcePackageId,
            createdAt = Some(f1.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          )
        )
      )
    }

    "work with the /public path prefix" in {
      val expectedPackageId = "N:package:1"
      val expected = createPackage(expectedPackageId)

      Get(s"/public/packages/$expectedPackageId/files") ~> serverRoutes ~> check {
        status should be(StatusCodes.OK)
        responseAs[FileTreeWithOrgPage] should be(expected)

      }
    }

    "work without the /public path prefix" in {
      val expectedPackageId = "N:package:1"
      val expected = createPackage(expectedPackageId)
      Get(s"/packages/$expectedPackageId/files") ~> serverRoutes ~> check {
        status should be(StatusCodes.OK)
        responseAs[FileTreeWithOrgPage] should be(expected)

      }
    }

  }

}
