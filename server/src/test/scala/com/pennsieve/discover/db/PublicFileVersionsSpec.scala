// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.github.tminglei.slickpg.LTree
import com.pennsieve.discover.models.{ FileTreeNode, PublicFileVersion, S3Key }
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.models.{ File, FileManifest, FileType }
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.dbio.{ DBIOAction, NoStream }

import scala.concurrent.duration._
import com.pennsieve.discover.db.profile.api._

class PublicFileVersionsSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  def run[A](
    dbio: DBIOAction[A, NoStream, Nothing],
    timeout: FiniteDuration = 5.seconds
  ) =
    ports.db.run(dbio).awaitFinite(timeout)

  "PublicFileVersionsMapper" should {
    "create a file version" in {
      val version = TestUtilities.createDatasetV1(ports.db)(migrated = true)

      val datasetId = version.datasetId
      val name = "file.dat"
      val s3Key = S3Key.File(s"${datasetId}/${name}")

      var f = run(
        PublicFileVersionsMapper
          .create(
            PublicFileVersion(
              id = 1,
              name = name,
              fileType = "Text",
              size = 1,
              sourcePackageId = Some("N:package:1"),
              sourceFileUUID = None,
              s3Key = s3Key,
              s3Version = "version12345",
              path = LTree(PublicFileVersionsMapper.convertPathToTree(s3Key)),
              datasetId = datasetId
            )
          )
      )

      run(PublicFileVersionsMapper.getAll(datasetId)).length shouldBe 1
    }

    "create and link a file version" in {
      val version = TestUtilities.createDatasetV1(ports.db)(migrated = true)

      val name = "test.dat"
      val path = "files/data/test.dat"

      run(
        PublicFileVersionsMapper.createAndLink(
          version,
          FileManifest(
            name = name,
            path = path,
            size = 1024,
            fileType = FileType.Text,
            sourcePackageId = Some("N:package:1"),
            id = None,
            s3VersionId = Some("version12345")
          )
        )
      )

      run(PublicFileVersionsMapper.getAll(version.datasetId)).length shouldBe 1

    }

    "create many file versions" in {
      val version = TestUtilities.createDatasetV1(ports.db)(migrated = true)

      run(
        PublicFileVersionsMapper.createMany(
          version,
          List(
            FileManifest(
              name = "test3.dat",
              path = "files/data/test3.dat",
              size = 1024,
              fileType = FileType.Text,
              sourcePackageId = Some("N:package:31"),
              id = None,
              s3VersionId = Some("version-00001")
            ),
            FileManifest(
              name = "test4.dat",
              path = "files/data/test4dat",
              size = 1024,
              fileType = FileType.Text,
              sourcePackageId = Some("N:package:32"),
              id = None,
              s3VersionId = Some("version-00002")
            )
          )
        )
      )

      run(PublicFileVersionsMapper.getAll(version.datasetId)).length shouldBe 2
    }

    "return dataset file version created_at time for files in getChildren" in {
      val version = TestUtilities.createDatasetV1(ports.db)(migrated = true)

      val name = "test.dat"
      val path = "files/data/test.dat"

      val publicFileVersion = run(
        PublicFileVersionsMapper
          .createAndLink(
            version,
            FileManifest(
              name = name,
              path = path,
              size = 1024,
              fileType = FileType.Text,
              sourcePackageId = Some("N:package:1"),
              id = None,
              s3VersionId = Some("version12345")
            )
          )
      )

      val publicDatasetVersionFile = run(
        PublicDatasetVersionFilesTableMapper
          .filter(_.fileId === publicFileVersion.id)
          .result
          .head
      )

      val (_, children) = run(
        PublicFileVersionsMapper
          .childrenOf(version = version, path = Some("files/data/"))
      )

      children.length shouldBe 1

      children.head shouldBe a[FileTreeNode.File]

      children.head
        .asInstanceOf[FileTreeNode.File]
        .createdAt shouldBe Some(publicDatasetVersionFile.createdAt)
    }

  }

  "PublicDatasetVersionFilesMapper" should {
    "store a link" in {
      val version = TestUtilities.createDatasetV1(ports.db)(migrated = true)

      val datasetId = version.datasetId
      val name = "file.dat"
      val s3Key = S3Key.File(s"${datasetId}/${name}")

      val fileVersion = run(
        PublicFileVersionsMapper
          .create(
            PublicFileVersion(
              id = 1,
              name = name,
              fileType = "Text",
              size = 1,
              sourcePackageId = Some("N:package:1"),
              sourceFileUUID = None,
              s3Key = s3Key,
              s3Version = "version12345",
              path = LTree(PublicFileVersionsMapper.convertPathToTree(s3Key)),
              datasetId = datasetId
            )
          )
      )

      val link = run(
        PublicDatasetVersionFilesTableMapper.storeLink(version, fileVersion)
      )

      link.datasetId shouldEqual (version.datasetId)
      link.datasetVersion shouldEqual (version.version)
      link.fileId shouldEqual (fileVersion.id)
    }
  }

}
