// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import com.pennsieve.discover.client.file.{
  FileClient,
  GetFileFromSourcePackageIdResponse
}
import com.pennsieve.models.{ FileType, Icon, PackageType }
import org.scalatest.{ Matchers, WordSpec }
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{ RouteTestTimeout, ScalatestRouteTest }
import com.pennsieve.discover._
import com.pennsieve.discover.TestUtilities._
import com.pennsieve.models.PublishStatus.{
  PublishInProgress,
  PublishSucceeded
}
import com.pennsieve.models.PublishStatus

class FileHandlerSpec
    extends WordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness
    with TempDirectoryFixture {

  def createRoutes(): Route =
    Route.seal(FileHandler.routes(ports))

  def createClient(routes: Route): FileClient =
    FileClient.httpClient(Route.asyncHandler(routes))

  val fileClient: FileClient = createClient(createRoutes())

  "GET /files/{sourcePackageId}" should {

    "return the files of the latest version of a dataset when a valid sourcePackageId is passed" in {
      val expectedOrgId = 3
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = expectedOrgId,
        status = PublishStatus.PublishSucceeded
      )

      val v2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = v1.datasetId,
        status = PublishSucceeded
      )

      val f1 = TestUtilities.createFile(ports.db)(
        v2,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val f2 = TestUtilities.createFile(ports.db)(
        v2,
        "A/file2.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val f3 = TestUtilities.createFile(ports.db)(
        v1,
        "A/file3.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = fileClient
        .getFileFromSourcePackageId("N:package:1")
        .awaitFinite()

      val expected = client.definitions.FileTreeWithOrgPage(
        totalCount = 2,
        limit = 100,
        offset = 0,
        organizationId = expectedOrgId,
        files = IndexedSeq(
          client.definitions.File(
            name = f1.name,
            path = "A/file1.txt",
            size = f1.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f1.s3Key.toString,
            sourcePackageId = f1.sourcePackageId,
            createdAt = Some(f1.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          ),
          client.definitions.File(
            name = f2.name,
            path = "A/file2.txt",
            size = f2.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f2.s3Key.toString,
            sourcePackageId = f2.sourcePackageId,
            createdAt = Some(f2.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          )
        )
      )

      response shouldBe Right(GetFileFromSourcePackageIdResponse.OK(expected))
    }

    "respect limit and offset and return the files of the latest version of a dataset when a valid sourcePackageId is passed" in {
      val expectedOrgId = 4
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        sourceOrganizationId = expectedOrgId,
        status = PublishStatus.PublishSucceeded
      )

      val f1 = TestUtilities.createFile(ports.db)(
        v1,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val f2 = TestUtilities.createFile(ports.db)(
        v1,
        "A/file2.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val f3 = TestUtilities.createFile(ports.db)(
        v1,
        "A/file3.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = fileClient
        .getFileFromSourcePackageId(
          "N:package:1",
          limit = Some(2),
          offset = Some(1)
        )
        .awaitFinite()

      val expected = client.definitions.FileTreeWithOrgPage(
        totalCount = 3,
        limit = 2,
        offset = 1,
        organizationId = expectedOrgId,
        files = IndexedSeq(
          client.definitions.File(
            name = f2.name,
            path = "A/file2.txt",
            size = f2.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f2.s3Key.toString,
            sourcePackageId = f2.sourcePackageId,
            createdAt = Some(f2.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          ),
          client.definitions.File(
            name = f3.name,
            path = "A/file3.txt",
            size = f3.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f3.s3Key.toString,
            sourcePackageId = f3.sourcePackageId,
            createdAt = Some(f3.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          )
        )
      )

      response shouldBe Right(GetFileFromSourcePackageIdResponse.OK(expected))
    }

    "return 404 when a invalid sourcePackageId is passed" in {
      val v = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded
      )

      val f = TestUtilities.createFile(ports.db)(
        v,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = fileClient
        .getFileFromSourcePackageId("N:package:2")
        .awaitFinite()

      val expected =
        List(
          client.definitions.File(
            name = f.name,
            path = "A/file1.txt",
            size = f.size,
            uri = "s3://" + config.s3.publishBucket + "/" + f.s3Key.toString,
            sourcePackageId = f.sourcePackageId,
            createdAt = Some(f.createdAt),
            fileType = FileType.Text,
            packageType = PackageType.Text,
            icon = utils.getIcon(FileType.Text)
          )
        )
      response shouldBe Right(
        GetFileFromSourcePackageIdResponse.NotFound("N:package:2")
      )
    }

    "return 404 when a valid sourcePackageId is passed but the file is not in the latest version" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.PublishSucceeded
      )

      val v2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = v1.datasetId,
        status = PublishSucceeded
      )

      val f = TestUtilities.createFile(ports.db)(
        v1,
        "A/file1.txt",
        "TEXT",
        sourcePackageId = Some("N:package:1")
      )

      val response = fileClient
        .getFileFromSourcePackageId("N:package:1")
        .awaitFinite()

      response shouldBe Right(
        GetFileFromSourcePackageIdResponse.NotFound("N:package:1")
      )
    }
  }
}
