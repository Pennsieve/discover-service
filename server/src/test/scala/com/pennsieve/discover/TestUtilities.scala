// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.db._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models._
import com.pennsieve.models.{
  Degree,
  FileManifest,
  FileType,
  License,
  PublishStatus
}
import com.pennsieve.test.AwaitableImplicits
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.exceptions.DockerException
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory

import java.nio.file.{ Files, Path }
import java.util.UUID
import java.util.Comparator
import java.time.{ LocalDate, LocalTime, OffsetDateTime, ZoneOffset }
import scala.concurrent.ExecutionContext
import scala.sys.process._
import scala.util.Random

object TestUtilities extends AwaitableImplicits {

  /**
    * Create a temporary directory, and clean it up after the test is done.
    */
  trait TempDirectoryFixture {
    def withTempDirectory(testCode: Path => Any) = {
      val tempDir: Path = Files.createTempDirectory(UUID.randomUUID().toString)
      try {
        testCode(tempDir)
      } finally {
        Files
          .walk(tempDir)
          .sorted(Comparator.reverseOrder())
          .peek(path => println(s"Deleting $path"))
          .forEach(Files.delete)
      }
    }
  }

  /**
    * Unpack a ZIP archive.
    */
  def unzipArchive(archive: String, dest: String): Unit = {
    println(Seq("unzip", "-l", archive).!!)
    val returnVal = Seq("unzip", archive, "-d", dest).!
    assert(returnVal == 0)
  }

  def randomString(length: Int = 20): String =
    Random.alphanumeric take length mkString

  def randomInteger(max: Int = 100): Int =
    Random.nextInt(max)

  def createDataset(
    db: Database
  )(
    name: String = "My Dataset",
    sourceOrganizationId: Int = 1,
    sourceOrganizationName: String = "Pennsieve",
    sourceDatasetId: Int = 1,
    ownerId: Int = 1,
    ownerFirstName: String = "Fynn",
    ownerLastName: String = "Blackwell",
    ownerOrcid: String = "0000-0001-2345-6789",
    license: License = License.`Apache 2.0`,
    tags: List[String] = List(randomString(), randomString())
  )(implicit
    executionContext: ExecutionContext
  ): PublicDataset = {
    db.run(
        PublicDatasetsMapper.createOrUpdate(
          name = name,
          sourceOrganizationId = sourceOrganizationId,
          sourceOrganizationName = sourceOrganizationName,
          sourceDatasetId = sourceDatasetId,
          ownerId = ownerId,
          ownerFirstName = ownerFirstName,
          ownerLastName = ownerLastName,
          ownerOrcid = ownerOrcid,
          license = license,
          tags = tags
        )
      )
      .await

  }

  def createDatasetV1(
    db: Database
  )(
    name: String = "My Dataset",
    sourceOrganizationId: Int = 1,
    sourceOrganizationName: String = "Pennsieve",
    sourceDatasetId: Int = 1,
    description: String = "this is a test",
    license: License = License.`Apache 2.0`,
    tags: List[String] = List(randomString(), randomString()),
    ownerId: Int = 1,
    ownerFirstName: String = "Fynn",
    ownerLastName: String = "Blackwell",
    ownerOrcid: String = "0000-0001-2345-6789",
    size: Long = 1000L,
    modelCount: Map[String, Long] = Map.empty[String, Long],
    fileCount: Long = 0L,
    recordCount: Long = 0L,
    s3Bucket: String = "bucket",
    status: PublishStatus = PublishStatus.NotPublished,
    doi: String = randomString(),
    embargoReleaseDate: Option[LocalDate] = None,
    migrated: Boolean = false
  )(implicit
    executionContext: ExecutionContext
  ): PublicDatasetVersion = {
    val publicDataset = createDataset(db)(
      name,
      sourceOrganizationId,
      sourceOrganizationName,
      sourceDatasetId,
      ownerId,
      ownerFirstName,
      ownerLastName,
      ownerOrcid,
      license,
      tags
    )

    db.run(
        PublicDatasetVersionsMapper.create(
          id = publicDataset.id,
          size = size,
          description = description,
          modelCount = modelCount,
          fileCount = fileCount,
          recordCount = recordCount,
          s3Bucket = S3Bucket(s3Bucket),
          status = status,
          doi = doi,
          schemaVersion = PennsieveSchemaVersion.`4.0`,
          banner = Some(S3Key.File("path-to-banner")),
          readme = Some(S3Key.File("path-to-readme")),
          embargoReleaseDate = embargoReleaseDate,
          migrated = migrated
        )
      )
      .await
  }

  def metadataFileManifest(version: PublicDatasetVersion): FileManifest =
    FileManifest(
      name = DatasetMetadata.metadataFileName(version),
      path = DatasetMetadata.metadataFileName(version),
      size = 1234,
      fileType = FileType.Json,
      sourcePackageId = None,
      id = None,
      s3VersionId = version.migrated match {
        case true => Some("Fake1234")
        case false => None
      }
    )

  def addMetadata(
    db: Database,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): Unit =
    version.migrated match {
      case true =>
        db.run(
            PublicFileVersionsMapper
              .createAndLink(version, metadataFileManifest(version))
          )
          .await
      case false =>
        db.run(
            PublicFilesMapper
              .createMany(version, List(metadataFileManifest(version)))
          )
          .await
    }

  def createNewDatasetVersion(
    db: Database
  )(
    id: Int,
    size: Long = 10000L,
    description: String = "another version",
    modelCount: Map[String, Long] = Map[String, Long](randomString() -> 10L),
    fileCount: Long = 100L,
    recordCount: Long = 10L,
    status: PublishStatus = PublishStatus.PublishInProgress,
    doi: String = randomString(),
    embargoReleaseDate: Option[LocalDate] = None,
    migrated: Boolean = false
  )(implicit
    executionContext: ExecutionContext
  ): PublicDatasetVersion = {
    db.run(
        PublicDatasetVersionsMapper.create(
          id = id,
          size = size,
          description = description,
          modelCount = modelCount,
          fileCount = fileCount,
          recordCount = recordCount,
          s3Bucket = S3Bucket(if (PublicDatasetVersion.underEmbargo(status)) {
            "embargo-bucket"
          } else {
            "bucket"
          }),
          status = status,
          doi = doi,
          schemaVersion = PennsieveSchemaVersion.`4.0`,
          banner = Some(S3Key.File("path-to-banner")),
          readme = Some(S3Key.File("path-to-readme")),
          embargoReleaseDate = embargoReleaseDate,
          migrated = migrated
        )
      )
      .await
  }

  def createRevision(
    db: Database
  )(
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): Revision = {
    db.run(RevisionsMapper.create(version)).await
  }

  def createContributor(
    db: Database
  )(
    firstName: String = "Test",
    middleInitial: Option[String] = Some("G"),
    lastName: String = "Contributor",
    degree: Option[Degree] = Some(Degree.BS),
    orcid: Option[String],
    datasetId: Int,
    organizationId: Int,
    version: Int,
    sourceContributorId: Int = randomInteger(100),
    sourceUserId: Option[Int] = None
  )(implicit
    executionContext: ExecutionContext
  ): PublicContributor = {
    db.run(
        PublicContributorsMapper.create(
          firstName = firstName,
          middleInitial = middleInitial,
          lastName = lastName,
          degree = degree,
          orcid = orcid,
          datasetId = datasetId,
          version = version,
          sourceContributorId = sourceContributorId,
          sourceUserId = sourceUserId
        )
      )
      .await
  }

  def createCollection(
    db: Database
  )(
    name: String = "Test",
    datasetId: Int,
    version: Int,
    sourceCollectionId: Int
  )(implicit
    executionContext: ExecutionContext
  ): PublicCollection = {
    db.run(
        PublicCollectionsMapper.create(
          name = name,
          datasetId = datasetId,
          version = version,
          sourceCollectionId = sourceCollectionId
        )
      )
      .await
  }

  def createFile(
    db: Database
  )(
    version: PublicDatasetVersion,
    path: String,
    fileType: String,
    size: Long = 100,
    sourcePackageId: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): PublicFile =
    db.run(
        PublicFilesMapper.create(
          version = version,
          name = path.split("/").last,
          fileType = fileType,
          size = size,
          s3Key = version.s3Key / path,
          sourcePackageId = sourcePackageId
        )
      )
      .awaitFinite()

  def createFileVersion(
    db: Database
  )(
    version: PublicDatasetVersion,
    path: String,
    fileType: FileType,
    size: Long = 100,
    sourcePackageId: Option[String] = None,
    s3Version: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): PublicFileVersion =
    db.run(
        PublicFileVersionsMapper.createAndLink(
          version,
          FileManifest(
            name = path.split("/").last,
            path = path,
            size = size,
            fileType = fileType,
            sourcePackageId = sourcePackageId,
            id = None,
            s3VersionId = s3Version
          )
        )
      )
      .awaitFinite()

  def createDatasetDownloadRow(
    db: Database
  )(
    datasetId: Int,
    version: Int,
    downloadOrigin: DownloadOrigin,
    requestID: Option[String] = None,
    dateTime: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
  )(implicit
    executionContext: ExecutionContext
  ): DatasetDownload = {
    db.run(
        DatasetDownloadsMapper
          .create(datasetId, version, Some(downloadOrigin), requestID, dateTime)
      )
      .await
  }

  def getDatasetDownloads(
    db: Database
  )(
    startDate: LocalDate,
    endDate: LocalDate
  )(implicit
    executionContext: ExecutionContext
  ) = {
    db.run(
        DatasetDownloadsMapper.getDatasetDownloadsByDateRange(
          OffsetDateTime.of(startDate, LocalTime.MIDNIGHT, ZoneOffset.UTC),
          OffsetDateTime.of(endDate, LocalTime.MIDNIGHT, ZoneOffset.UTC)
        )
      )
      .await
  }

  /**
    * Our tests use a Whisk library, that in turn uses a Spotify library, to manage
    * docker containers created for tests. The Spotify part of this is no longer updated and breaks
    * for newer versions of the Docker API. This creates a DockerFactory set to create clients that
    * use a version of the API that works for us.
    *
    * @return a Spotify Docker client that will use version 1.41 of the Docker API
    */
  def dockerFactoryApiVersion141: DockerFactory = {
    try new SpotifyDockerFactory(
      DefaultDockerClient
        .fromEnv()
        .apiVersion("v1.41")
        .build()
    )
    catch {
      case _: DockerException =>
        throw new DockerException("Docker may not be running")
    }
  }
}
