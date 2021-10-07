// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.server.definitions.DatasetPublishStatus
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db.PublicFilesMapper.TotalCount
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.models.{
  FileDownloadDTO,
  FileTreeNode,
  PublicDataset,
  PublicFile,
  S3Key
}
import com.pennsieve.models.{ FileManifest, FileType }
import com.pennsieve.test.AwaitableImplicits
import io.circe.syntax._
import org.scalatest.{ Matchers, WordSpec }

import scala.concurrent.duration._

class PublicFilesMapperSpec
    extends WordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  def run[A](
    dbio: DBIOAction[A, NoStream, Nothing],
    timeout: FiniteDuration = 5.seconds
  ) =
    ports.db.run(dbio).awaitFinite(timeout)

  "PublicFilesMapper" should {

    "find files and directories in the tree" in {

      val version = TestUtilities.createDatasetV1(ports.db)()

      val f1 = run(
        PublicFilesMapper
          .create(
            version,
            "zfile1.zip",
            "ZIP",
            100,
            version.s3Key / "A/file1.zip",
            Some("N:package:1")
          )
      )
      val f2 = run(
        PublicFilesMapper
          .create(
            version,
            "file2.txt",
            "TEXT",
            100,
            version.s3Key / "A/file2.txt",
            Some("N:package:1")
          )
      )
      val f3 = run(
        PublicFilesMapper
          .create(
            version,
            "file3.dcm",
            "DICOM",
            100,
            version.s3Key / "A/Z/file3.dcm",
            Some("N:package:2")
          )
      )
      val f4 = run(
        PublicFilesMapper
          .create(
            version,
            "file4.csv",
            "CSV",
            100,
            S3Key.File("1000/2222/A/C/file4.csv"),
            Some("N:package:3")
          )
      )

      val f5 = run(
        PublicFilesMapper
          .create(
            version,
            "file5.m",
            "MATLAB",
            100,
            version.s3Key / "A/Y/file5.m",
            Some("N:package:4")
          )
      )

      // No directory path => top level of the dataset version
      run(PublicFilesMapper.childrenOf(version, None)) shouldBe (
        (
          TotalCount(1),
          Seq(FileTreeNode.Directory("A", "A", 400))
        )
      )

      // Can specify subpaths of the version
      // Directories should be returned first
      run(PublicFilesMapper.childrenOf(version, Some("A"))) shouldBe (
        (
          TotalCount(4),
          Seq(
            FileTreeNode.Directory("Y", "A/Y", 100),
            FileTreeNode.Directory("Z", "A/Z", 100),
            FileTreeNode
              .File(
                "file2.txt",
                "A/file2.txt",
                FileType.Text,
                f2.s3Key,
                100,
                Some("N:package:1")
              ),
            FileTreeNode
              .File(
                "zfile1.zip",
                "A/zfile1.zip",
                FileType.ZIP,
                f1.s3Key,
                100,
                Some("N:package:1")
              )
          )
        )
      )

      // Limit/offset work
      run(
        PublicFilesMapper.childrenOf(version, Some("A"), limit = 2, offset = 1)
      ) shouldBe (
        (
          TotalCount(4),
          Seq(
            FileTreeNode.Directory("Z", "A/Z", 100),
            FileTreeNode.File(
              "file2.txt",
              "A/file2.txt",
              FileType.Text,
              f2.s3Key,
              100,
              Some("N:package:1")
            )
          )
        )
      )

      run(PublicFilesMapper.childrenOf(version, Some("A/Z"))) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode
              .File(
                "file3.dcm",
                "A/Z/file3.dcm",
                FileType.DICOM,
                f3.s3Key,
                100,
                Some("N:package:2")
              )
          )
        )
      )

      // Can handle extra trailing slashes
      run(PublicFilesMapper.childrenOf(version, Some("A/Z//"))) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode
              .File(
                "file3.dcm",
                "A/Z/file3.dcm",
                FileType.DICOM,
                f3.s3Key,
                100,
                Some("N:package:2")
              )
          )
        )
      )

      // And extra leading slashes
      run(PublicFilesMapper.childrenOf(version, Some("//A/Z"))) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode
              .File(
                "file3.dcm",
                "A/Z/file3.dcm",
                FileType.DICOM,
                f3.s3Key,
                100,
                Some("N:package:2")
              )
          )
        )
      )

      // Files of another version should not be accesible
      run(PublicFilesMapper.childrenOf(version, Some("Z/file3.dcm"))) shouldBe (
        (
          TotalCount(0),
          Seq.empty
        )
      )
    }

    "find all files (for download) matching an array of paths" in {

      val version = TestUtilities.createDatasetV1(ports.db)()

      val f1 = run(
        PublicFilesMapper
          .create(
            version,
            "zfile1.zip",
            "ZIP",
            100,
            version.s3Key / "A/file1.zip",
            Some("N:package:1")
          )
      )
      val f2 = run(
        PublicFilesMapper
          .create(
            version,
            "file2.txt",
            "TEXT",
            100,
            version.s3Key / "A/file2.txt",
            Some("N:package:1")
          )
      )
      val f3 = run(
        PublicFilesMapper
          .create(
            version,
            "file3.dcm",
            "DICOM",
            100,
            version.s3Key / "A/Z/file3.dcm",
            Some("N:package:2")
          )
      )
      val f4 = run(
        PublicFilesMapper
          .create(
            version,
            "file4.csv",
            "CSV",
            100,
            S3Key.File("1000/2222/A/C/file4.csv"),
            Some("N:package:3")
          )
      )

      val f5 = run(
        PublicFilesMapper
          .create(
            version,
            "file5.m",
            "MATLAB",
            100,
            version.s3Key / "A/Y/file5.m",
            Some("N:package:4")
          )
      )

      run(
        PublicFilesMapper
          .getFileDownloadsMatchingPaths(version, Seq("A/Y/file5.m", "A/Z"))
      ) shouldBe {
        Seq(
          FileDownloadDTO(version, f3.name, f3.s3Key, f3.size),
          FileDownloadDTO(version, f5.name, f5.s3Key, f5.size)
        )
      }

    }

    "insert multiple file manifests" in {

      val version = TestUtilities.createDatasetV1(ports.db)()

      run(
        PublicFilesMapper.createMany(
          version,
          List(
            FileManifest("A/file1.zip", 100, FileType.ZIP, None),
            FileManifest("A/file2.txt", 200, FileType.Text, None)
          )
        )
      )

      run(PublicFilesMapper.forVersion(version).result).length shouldBe 2
    }

    "insert 100,000 file manifests without an OOM " in {
      val version = TestUtilities.createDatasetV1(ports.db)()
      run(
        PublicFilesMapper.createMany(
          version,
          List
            .range(0, 100000)
            .map(i => FileManifest(s"file$i.zip", 100, FileType.ZIP, None))
        ),
        30.seconds
      )
      run(PublicFilesMapper.forVersion(version).length.result) shouldBe 100000
    }
  }
}
