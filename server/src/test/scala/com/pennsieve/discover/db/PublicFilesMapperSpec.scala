// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db.TotalCount
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.models.{
  FileDownloadDTO,
  FileTreeNode,
  S3Bucket,
  S3Key
}
import com.pennsieve.models.{ FileManifest, FileType }
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.ZoneOffset
import scala.concurrent.duration._

class PublicFilesMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  def run[A](
    dbio: DBIOAction[A, NoStream, Nothing],
    timeout: FiniteDuration = 5.seconds
  ) =
    ports.db.run(dbio).awaitFinite(timeout)
  val publishBucket = S3Bucket("bucket")

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

      val f1Actual =
        run(PublicFilesMapper.filter(_.s3Key === f1.s3Key).result.head)

      println("f1", f1.createdAt)
      println("f1Actual", f1Actual.createdAt)
      println("f1 as UTC", f1.createdAt.withOffsetSameInstant(ZoneOffset.UTC))

      f1Actual.createdAt shouldBe (f1.createdAt)

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
      val (count, childrenOfA) =
        run(PublicFilesMapper.childrenOf(version, Some("A")))
      count shouldBe TotalCount(4)

      val file2 = childrenOfA(2)

      file2
        .asInstanceOf[FileTreeNode.File]
        .createdAt
        .get
        .toInstant shouldBe f2.createdAt.toInstant

      childrenOfA shouldBe Seq(
        FileTreeNode.Directory("Y", "A/Y", 100),
        FileTreeNode.Directory("Z", "A/Z", 100),
        FileTreeNode
          .File(
            "file2.txt",
            "A/file2.txt",
            FileType.Text,
            f2.s3Key,
            publishBucket,
            100,
            Some("N:package:1"),
            Some(f2.createdAt)
          ),
        FileTreeNode
          .File(
            "zfile1.zip",
            "A/zfile1.zip",
            FileType.ZIP,
            f1.s3Key,
            publishBucket,
            100,
            Some("N:package:1"),
            Some(f1.createdAt)
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
              publishBucket,
              100,
              Some("N:package:1"),
              Some(f2.createdAt)
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
                publishBucket,
                100,
                Some("N:package:2"),
                Some(f3.createdAt)
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
                publishBucket,
                100,
                Some("N:package:2"),
                Some(f3.createdAt)
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
                publishBucket,
                100,
                Some("N:package:2"),
                Some(f3.createdAt)
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

    "insert and find a file that would contain a '/' in it's base64 path encoding " in {
      // The path component containing 'µ' was chosen because in our initial method
      // of ltree label creation it's label contained a '/' which Postgres does not allow
      // in labels. This test verifies that the new method of ltree label creation avoids
      // the '/' and results in a ltree path that still leaves the file findable.
      val version = TestUtilities.createDatasetV1(ports.db)()

      val expectedName = "0mmSD.mat"
      val expectedSize = 100
      val expectedPath =
        "files/Jacobians/µa_0.035_mm-1___µs_15_mm-1_/0mmSD.mat"
      val expectedS3Key = version.s3Key / expectedPath
      val expectedPackageId = Some("N:package:3")
      run(
        PublicFilesMapper.create(
          version,
          expectedName,
          "CSV",
          expectedSize,
          expectedS3Key,
          expectedPackageId
        )
      )

      run(PublicFilesMapper.childrenOf(version, None)) shouldBe (
        (
          TotalCount(1),
          Seq(FileTreeNode.Directory("files", "files", expectedSize))
        )
      )

      run(PublicFilesMapper.childrenOf(version, Some("files"))) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode.Directory("Jacobians", "files/Jacobians", expectedSize)
          )
        )
      )

      run(PublicFilesMapper.childrenOf(version, Some("files/Jacobians"))) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode.Directory(
              "µa_0.035_mm-1___µs_15_mm-1_",
              "files/Jacobians/µa_0.035_mm-1___µs_15_mm-1_",
              expectedSize
            )
          )
        )
      )

      run(
        PublicFilesMapper.childrenOf(
          version,
          Some("files/Jacobians/µa_0.035_mm-1___µs_15_mm-1_")
        )
      ) shouldBe (
        (
          TotalCount(1),
          Seq(
            FileTreeNode.File(
              expectedName,
              expectedPath,
              FileType.CSV,
              expectedS3Key,
              publishBucket,
              expectedSize,
              expectedPackageId
            )
          )
        )
      )

      run(
        PublicFilesMapper
          .getFileDownloadsMatchingPaths(version, Seq(expectedPath))
      ) shouldBe {
        Seq(FileDownloadDTO(version, expectedName, expectedS3Key, expectedSize))
      }
    }
  }
}
