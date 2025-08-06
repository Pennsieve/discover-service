// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.Done
import com.pennsieve.discover.client.definitions.InternalContributor
import com.pennsieve.discover.db._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models._
import com.pennsieve.models.{
  DatasetType,
  Degree,
  FileManifest,
  FileType,
  License,
  PublishStatus,
  RelationshipType
}
import com.pennsieve.test.AwaitableImplicits
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.exceptions.DockerException
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{ Files, Path }
import java.util.UUID
import java.util.Comparator
import java.time.{ LocalDate, LocalTime, OffsetDateTime, ZoneOffset }
import scala.concurrent.ExecutionContext
import scala.sys.process._
import scala.util.Random

object TestUtilities extends AwaitableImplicits {

  val defaultS3VersionId = "::S3VersionId::"

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
    tags: List[String] = List(randomString(), randomString()),
    datasetType: DatasetType = DatasetType.Research
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
          tags = tags,
          datasetType = datasetType
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
    migrated: Boolean = false,
    datasetType: DatasetType = DatasetType.Research
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
      tags,
      datasetType
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

  def createDatasetRelease(
    db: Database
  )(
    datasetId: Int,
    versionId: Int,
    origin: String,
    label: String,
    repoUrl: String,
    marker: String = randomString(32),
    labelUrl: Option[String] = None,
    markerUrl: Option[String] = None,
    releaseStatus: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): PublicDatasetRelease = {
    db.run(
        PublicDatasetReleaseMapper.add(
          PublicDatasetRelease(
            datasetId = datasetId,
            datasetVersion = versionId,
            origin = origin,
            label = label,
            marker = marker,
            repoUrl = repoUrl,
            labelUrl = labelUrl,
            markerUrl = markerUrl,
            releaseStatus = releaseStatus
          )
        )
      )
      .await
  }

  def addReleaseAssetFiles(
    db: Database
  )(
    version: PublicDatasetVersion,
    release: PublicDatasetRelease,
    listing: ReleaseAssetListing
  )(implicit
    executionContext: ExecutionContext
  ): Done = {
    db.run(
        PublicDatasetReleaseAssetMapper.createMany(version, release, listing)
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
    sourcePackageId: Option[String] = None,
    s3VersionId: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): PublicFileVersion =
    db.run(
        PublicFileVersionsMapper.createAndLink(
          version = version,
          FileManifest(
            path = path,
            size = size,
            fileType = FileType.Text,
            sourcePackageId = None,
            s3VersionId = version.migrated match {
              case true =>
                s3VersionId match {
                  case Some(s3VersionId) => Some(s3VersionId)
                  case None => Some(defaultS3VersionId)
                }
              case false => None
            }
          )
        )
      )
      .awaitFinite()

  def createFileWithSha256(
    db: Database
  )(
    version: PublicDatasetVersion,
    path: String,
    fileType: String,
    size: Long = 100,
    sourcePackageId: Option[String] = None,
    s3VersionId: Option[String] = None,
    sha256: String
  )(implicit
    executionContext: ExecutionContext
  ): PublicFileVersion =
    db.run(
        PublicFileVersionsMapper.createAndLink(
          version = version,
          FileManifest(
            path = path,
            size = size,
            fileType = FileType.Text,
            sourcePackageId = None,
            s3VersionId = version.migrated match {
              case true =>
                s3VersionId match {
                  case Some(s3VersionId) => Some(s3VersionId)
                  case None => Some(defaultS3VersionId)
                }
              case false => None
            }
          ).withSHA256(sha256)
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

  def createSponsorship(
    db: Database
  )(
    sourceOrganizationId: Int,
    sourceDatasetId: Int,
    title: Option[String] = Some(randomString()),
    imageUrl: Option[String] = Some(randomString()),
    markup: Option[String] = Some(randomString())
  )(implicit
    executionContext: ExecutionContext
  ): Sponsorship =
    db.run(
        SponsorshipsMapper.createOrUpdate(
          sourceOrganizationId,
          sourceDatasetId,
          title,
          imageUrl,
          markup
        )
      )
      .awaitFinite()

  def createExternalPublication(
    db: Database
  )(
    datasetId: Int,
    version: Int,
    relationshipType: RelationshipType,
    doi: String = randomString()
  )(implicit
    executionContext: ExecutionContext
  ): PublicExternalPublication =
    db.run(
        PublicExternalPublicationsMapper
          .create(doi, relationshipType, datasetId, version)
      )
      .awaitFinite()

  def assetFiles(): List[FileManifest] = List(
    FileManifest(
      name = "banner.jpg",
      path = "banner.jpg",
      size = TestUtilities.randomInteger(16 * 1024),
      fileType = FileType.JPEG,
      sourcePackageId = None,
      id = None,
      s3VersionId = Some(TestUtilities.randomString()),
      sha256 = Some(TestUtilities.randomString())
    ),
    FileManifest(
      name = "readme.md",
      path = "readme.md",
      size = TestUtilities.randomInteger(16 * 1024),
      fileType = FileType.Markdown,
      sourcePackageId = None,
      id = None,
      s3VersionId = Some(TestUtilities.randomString()),
      sha256 = Some(TestUtilities.randomString())
    ),
    FileManifest(
      name = "changelog.md",
      path = "changelog.md",
      size = TestUtilities.randomInteger(16 * 1024),
      fileType = FileType.Markdown,
      sourcePackageId = None,
      id = None,
      s3VersionId = Some(TestUtilities.randomString()),
      sha256 = Some(TestUtilities.randomString())
    ),
    FileManifest(
      name = "manifest.json",
      path = "manifest.json",
      size = TestUtilities.randomInteger(16 * 1024),
      fileType = FileType.Json,
      sourcePackageId = None,
      id = None,
      s3VersionId = Some(TestUtilities.randomString()),
      sha256 = Some(TestUtilities.randomString())
    )
  )

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

  def createDoiCollectionDataset(
    db: Database,
    idSpace: IdSpace
  )(
    name: String = "My Dataset",
    sourceDatasetId: Int = 1,
    ownerId: Int = 1,
    ownerFirstName: String = "Fynn",
    ownerLastName: String = "Blackwell",
    ownerOrcid: String = "0000-0001-2345-6789",
    license: License = License.`Apache 2.0`
  )(implicit
    executionContext: ExecutionContext
  ): PublicDataset = {
    db.run(
        PublicDatasetsMapper.createOrUpdate(
          name = name,
          sourceOrganizationId = idSpace.id,
          sourceOrganizationName = idSpace.name,
          sourceDatasetId = sourceDatasetId,
          ownerId = ownerId,
          ownerFirstName = ownerFirstName,
          ownerLastName = ownerLastName,
          ownerOrcid = ownerOrcid,
          license = license,
          tags = List.empty,
          datasetType = DatasetType.Collection
        )
      )
      .await

  }

  def randomBannerUrls: List[String] =
    List(
      s"https://example.com/${randomString()}.png",
      s"https://images.example.com/${randomString()}/${randomString()}.jpg",
      s"https://example.com/${randomString()}.png",
      s"https://images.example.com/${randomString()}/${randomString()}.jpg"
    )

  def randomPennsieveDoi(doiCollections: DoiCollections): String =
    s"${doiCollections.pennsieveDoiPrefix}/${TestUtilities.randomString()}"

  def createDatasetDoiCollection(
    db: Database
  )(
    datasetId: Int,
    datasetVersion: Int,
    banners: List[String] = TestUtilities.randomBannerUrls
  )(implicit
    executionContext: ExecutionContext
  ): PublicDatasetDoiCollection = {
    db.run(
        PublicDatasetDoiCollectionsMapper.add(
          PublicDatasetDoiCollection(
            datasetId = datasetId,
            datasetVersion = datasetVersion,
            banners = banners
          )
        )
      )
      .await
  }

  def addDoiCollectionDois(
    db: Database
  )(
    doiCollection: PublicDatasetDoiCollection,
    dois: List[String]
  )(implicit
    executionContext: ExecutionContext
  ): PublicDatasetDoiCollectionWithSize = {
    db.run(
        PublicDatasetDoiCollectionDoisMapper
          .addDOIs(doiCollection.datasetId, doiCollection.datasetVersion, dois)
      )
      .await
    PublicDatasetDoiCollectionWithSize(
      id = doiCollection.id,
      datasetId = doiCollection.datasetId,
      datasetVersion = doiCollection.datasetVersion,
      banners = doiCollection.banners,
      size = dois.size,
      createdAt = doiCollection.createdAt,
      updatedAt = doiCollection.updatedAt
    )
  }

  def internalAndPublicContributorsMatch(
    datasetId: Int,
    versionId: Int,
    internal: List[InternalContributor],
    public: List[PublicContributor]
  ): Unit = {
    internal.size shouldBe public.size
    internal.zip(public).foreach {
      case (i, p) =>
        p.firstName shouldBe i.firstName
        p.middleInitial shouldBe i.middleInitial
        p.lastName shouldBe i.lastName
        p.degree shouldBe i.degree
        p.orcid shouldBe i.orcid
        p.datasetId shouldBe datasetId
        p.versionId shouldBe versionId
        p.sourceContributorId shouldBe i.id
        p.sourceUserId shouldBe i.userId
    }

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
