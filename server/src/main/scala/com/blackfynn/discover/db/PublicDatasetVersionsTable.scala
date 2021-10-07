// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import java.time.OffsetDateTime

import cats.Monoid
import cats.implicits._
import com.pennsieve.discover.{ db, _ }
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions.{
  DatasetPublishStatus,
  SponsorshipRequest
}
import com.pennsieve.models.PublishStatus
import com.pennsieve.models.PublishStatus.{
  NotPublished,
  PublishFailed,
  PublishSucceeded
}
import org.postgresql.util.PSQLException
import slick.dbio.{ DBIOAction, Effect }
import slick.sql.{ FixedSqlAction, FixedSqlStreamingAction, SqlAction }

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }
import java.time.LocalDate

final class PublicDatasetVersionsTable(tag: Tag)
    extends Table[PublicDatasetVersion](tag, "public_dataset_versions") {

  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def size = column[Long]("total_size")
  def description = column[String]("description")
  def modelCount = column[Map[String, Long]]("model_count")
  def fileCount = column[Long]("file_count")
  def recordCount = column[Long]("record_count")
  def s3Bucket = column[S3Bucket]("s3_bucket")
  def s3Key = column[S3Key.Version]("s3_key")
  def status = column[PublishStatus]("status")
  def doi = column[String]("doi")
  def schemaVersion = column[PennsieveSchemaVersion]("schema_version")
  // banner and readme live in a public assets bucket and in the publish
  // bucket, and are accesible via the `assetsUrl` Cloudfront distribution:
  // (assets.discover.pennsieve.*)
  def banner = column[Option[S3Key.File]]("banner")
  def readme = column[Option[S3Key.File]]("readme")

  def executionArn = column[Option[String]]("execution_arn")
  def releaseExecutionArn = column[Option[String]]("release_execution_arn")

  def embargoReleaseDate = column[Option[LocalDate]]("embargo_release_date")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def fileDownloadsCounter = column[Int]("file_downloads_counter")
  def datasetDownloadsCounter = column[Int]("dataset_downloads_counter")

  def pk =
    primaryKey("public_dataset_versions_pk", (datasetId, version))

  def * =
    (
      datasetId,
      version,
      size,
      description,
      modelCount,
      fileCount,
      recordCount,
      s3Bucket,
      s3Key,
      status,
      doi,
      schemaVersion,
      banner,
      readme,
      executionArn,
      releaseExecutionArn,
      embargoReleaseDate,
      fileDownloadsCounter,
      datasetDownloadsCounter,
      createdAt,
      updatedAt
    ).mapTo[PublicDatasetVersion]
}

object PublicDatasetVersionsMapper
    extends TableQuery(new PublicDatasetVersionsTable(_)) {

  val visibleStates = Seq(
    PublishStatus.PublishSucceeded,
    PublishStatus.EmbargoSucceeded,
    PublishStatus.ReleaseInProgress,
    PublishStatus.ReleaseFailed,
    PublishStatus.Unpublished
  )

  val inMotionStates = Seq(
    PublishStatus.PublishInProgress,
    PublishStatus.EmbargoInProgress,
    PublishStatus.ReleaseInProgress
  )

  val successfulStates =
    Seq(PublishStatus.PublishSucceeded, PublishStatus.EmbargoSucceeded)

  val failedStates =
    Seq(PublishStatus.PublishFailed, PublishStatus.EmbargoFailed)

  def getVersion(
    id: Int,
    version: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetVersion, NoStream, Effect.Read] =
    this
      .filter(_.datasetId === id)
      .filter(_.version === version)
      .result
      .headOption
      .flatMap {
        case None => DBIO.failed(NoDatasetVersionException(id, version))
        case Some(dataset) => DBIO.successful(dataset)
      }

  def getVersionByDoi(
    doi: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[(PublicDataset, PublicDatasetVersion), NoStream, Effect.Read] =
    PublicDatasetsMapper
      .join(this.filter(_.doi === doi))
      .on(_.id === _.datasetId)
      .take(1)
      .result
      .headOption
      .flatMap {
        case Some((d, v))
            if v.status in Seq(
              PublishStatus.PublishSucceeded,
              PublishStatus.EmbargoSucceeded
            ) =>
          DBIO.successful((d, v))
        case Some((d, v)) if v.status == PublishStatus.Unpublished =>
          DBIO.failed(DatasetUnpublishedException(d, v))
        case _ =>
          DBIO.failed(NoDatasetForDoiException(doi))
      }

  def getVersionCounts(
    status: PublishStatus = PublishSucceeded
  ): Query[(Rep[Int], Rep[Int]), (Int, Int), Seq] = {
    this
      .filter(_.status === status)
      .groupBy(_.datasetId)
      .map {
        case (datasetId, versions) => (datasetId, versions.length)
      }
  }

  def getSuccessfulVersions(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.status inSet successfulStates)

  def getLatestVersion(
    id: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[PublicDatasetVersion], NoStream, Effect.Read] =
    this
      .filter(_.datasetId === id)
      .sortBy(_.version.desc)
      .take(1)
      .result
      .headOption

  /**
    * Check if the dataset is currently being published, embargoed, or released.
    */
  def isPublishing(dataset: PublicDataset): DBIO[Boolean] =
    this
      .filter(_.datasetId === dataset.id)
      .filter(_.status inSet inMotionStates)
      .exists
      .result

  /**
    * Visible datasets versions are ones that should be publicly accessible via
    * the Discover app and API.
    */
  def visibleVersions
    : Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    this.filter(_.status inSet visibleStates)

  def getLatestVisibleVersion(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[PublicDatasetVersion], NoStream, Effect.Read] =
    visibleVersions
      .filter(_.datasetId === dataset.id)
      .sortBy(_.version.desc)
      .take(1)
      .result
      .headOption

  def getVisibleVersion(
    dataset: PublicDataset,
    version: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetVersion, NoStream, Effect.Read with Effect] =
    visibleVersions
      .filter(_.datasetId === dataset.id)
      .filter(_.version === version)
      .result
      .headOption
      .flatMap {
        case Some(v) if v.status == PublishStatus.Unpublished =>
          DBIO.failed(DatasetUnpublishedException(dataset, v))
        case Some(v) => DBIO.successful(v)
        case _ =>
          DBIO.failed(NoDatasetVersionException(dataset.id, version))
      }

  def getLatestDatasetVersions(
    status: PublishStatus
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    getLatestDatasetVersions(List(status))

  def getLatestDatasetVersions(
    status: Seq[PublishStatus]
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] = {
    this
      .filter(version => version.status inSet status)
      .groupBy(version => version.datasetId)
      .map {
        case (datasetId, versions) =>
          (datasetId, versions.map(_.version).max)
      }
      .join(this)
      .on {
        case ((datasetId, version), dataset) =>
          dataset.datasetId === datasetId && dataset.version === version
      }
      .map(_._2)
      .sortBy(_.createdAt.desc)
  }

  def getLatestDatasetVersions
    : Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] = {
    this
      .groupBy(version => version.datasetId)
      .map {
        case (datasetId, versions) =>
          (datasetId, versions.map(_.version).max)
      }
      .join(this)
      .on {
        case ((datasetId, version), dataset) =>
          dataset.datasetId === datasetId && dataset.version === version
      }
      .map(_._2)
      .sortBy(_.createdAt.desc)
  }

  def getDatasetStatus(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    DatasetPublishStatus,
    NoStream,
    Effect.Read with Effect.Transactional
  ] = {
    datasetStatusForQuery(PublicDatasetsMapper.filter(_.id === dataset.id))
      .flatMap(_.headOption match {
        case Some(status) => DBIO.successful(status)
        case None => DBIO.failed(NoDatasetException(dataset.id))
      })
  }

  def getDatasetStatus(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    DatasetPublishStatus,
    NoStream,
    Effect.Read with Effect with Effect.Transactional
  ] =
    datasetStatusForQuery(
      PublicDatasetsMapper
        .filter(_.sourceOrganizationId === sourceOrganizationId)
        .filter(_.sourceDatasetId === sourceDatasetId)
    ).flatMap(_.headOption match {
      case Some(status) => DBIO.successful(status)
      case None => // this dataset has never been published
        DBIO.successful(
          DatasetPublishStatus(
            "",
            sourceOrganizationId,
            sourceDatasetId,
            None,
            0,
            PublishStatus.NotPublished,
            None
          )
        )
    })

  def getDatasetStatuses(
    sourceOrganizationId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Seq[DatasetPublishStatus], NoStream, Effect.Read with Effect.Transactional] =
    datasetStatusForQuery(
      PublicDatasetsMapper
        .getDatasetsByOrganization(sourceOrganizationId)
    )

  /**
    * Aggregate the `DatasetStatus` for thr public datasets in the given query.
    *
    * This computes the latest published version, status of the last attempted
    * publish job, and number of successfully published versions.
    */
  def datasetStatusForQuery(
    baseQuery: Query[PublicDatasetsTable, PublicDataset, Seq]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Seq[DatasetPublishStatus], NoStream, Effect.Read with Effect.Transactional] = {

    val query = for {
      latestVersions <- baseQuery
      // Latest publish attempt, regardless of status
        .joinLeft(getLatestDatasetVersions)
        .on(_.id === _.datasetId)
        // Latest successfully published dataset
        .joinLeft(getLatestDatasetVersions(visibleStates))
        .on(_._1.id === _.datasetId)
        // Number of published versions for the dataset
        .joinLeft(getVersionCounts(PublishSucceeded))
        .on(_._1._1.id === _._1)
        // sponsorship info
        .joinLeft(SponsorshipsMapper)
        .on(_._1._1._1.id === _.datasetId)
        .result

    } yield
      latestVersions.map {
        case (
            (((dataset, latestVersion), latestPublished), numVersions),
            sponsorship
            ) =>
          DatasetPublishStatus(
            name = dataset.name,
            sourceOrganizationId = dataset.sourceOrganizationId,
            sourceDatasetId = dataset.sourceDatasetId,
            publishedDatasetId = latestPublished.map(_.datasetId),
            publishedVersionCount = numVersions.map(_._2).getOrElse(0),
            status = latestVersion
              .map(_.status)
              .getOrElse(PublishStatus.NotPublished),
            lastPublishedDate = latestPublished.map(_.createdAt),
            sponsorship = sponsorship.map {
              case Sponsorship(_, title, imageUrl, markup, _) =>
                SponsorshipRequest(title, imageUrl, markup)
            }
          )
      }

    query.transactionally
  }

  case class PagedDatasetsResult(
    limit: Int,
    offset: Int,
    totalCount: Long,
    datasets: List[
      (PublicDataset, PublicDatasetVersion, IndexedSeq[PublicContributor],
        Option[Sponsorship], Option[Revision], IndexedSeq[PublicCollection],
        IndexedSeq[PublicExternalPublication])
    ]
  )

  def getPagedDatasets(
    tags: Option[List[String]],
    embargo: Option[Boolean] = None,
    ids: Option[List[Int]] = None,
    limit: Int,
    offset: Int,
    orderBy: OrderBy,
    orderDirection: OrderDirection
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PagedDatasetsResult, NoStream, Effect.Read] = {

    val status = embargo match {
      case Some(true) =>
        List(
          PublishStatus.EmbargoSucceeded,
          PublishStatus.ReleaseInProgress,
          PublishStatus.ReleaseFailed
        )
      case Some(false) => List(PublishStatus.PublishSucceeded)
      case _ =>
        List(
          PublishStatus.PublishSucceeded,
          PublishStatus.EmbargoSucceeded,
          PublishStatus.ReleaseInProgress,
          PublishStatus.ReleaseFailed
        )
    }

    val latestDatasetVersions = getLatestDatasetVersions(status)

    val allDatasets = PublicDatasetsMapper
      .getDatasets(tags, ids)
      .join(latestDatasetVersions)
      .on(_.id === _.datasetId)
      .joinLeft(SponsorshipsMapper)
      .on(_._1.id === _.datasetId)

    val sortedDatasets = allDatasets
      .sortBy {
        case ((dataset, version), _) =>
          (orderBy, orderDirection) match {
            case (OrderBy.Name, OrderDirection.Descending) =>
              dataset.name.desc
            case (OrderBy.Name, OrderDirection.Ascending) => dataset.name.asc
            case (OrderBy.Size, OrderDirection.Descending) =>
              version.size.desc
            case (OrderBy.Size, OrderDirection.Ascending) => version.size.asc
            // `Relevance` only makes sense for ElasticSearch. If order by
            // `Relevance` is requested, order by `Date` instead.
            case (_, OrderDirection.Descending) => version.createdAt.desc
            case (_, OrderDirection.Ascending) => version.createdAt.asc
          }
      }

    for {
      totalCount <- allDatasets.length.result.map(_.toLong)
      pagedDatasetsWithSponsorships <- sortedDatasets
        .drop(offset)
        .take(limit)
        .result
      contributorsMap <- PublicContributorsMapper.getDatasetContributors(
        pagedDatasetsWithSponsorships.map {
          case ((dataset, version), _) => (dataset, version)
        }
      )
      collectionsMap <- PublicCollectionsMapper.getDatasetCollections(
        pagedDatasetsWithSponsorships.map {
          case ((dataset, version), _) => (dataset, version)
        }
      )
      externalPublicationsMap <- PublicExternalPublicationsMapper
        .getExternalPublications(pagedDatasetsWithSponsorships.map {
          case ((dataset, version), _) => (dataset, version)
        })

      revisionsMap <- RevisionsMapper.getLatestRevisions(
        pagedDatasetsWithSponsorships.map {
          case ((_, version), _) => version
        }
      )
    } yield
      PagedDatasetsResult(
        limit,
        offset,
        totalCount,
        pagedDatasetsWithSponsorships.map {
          case ((dataset, version), sponsorship) =>
            (
              dataset,
              version,
              contributorsMap
                .get(dataset, version)
                .getOrElse(Nil)
                .toIndexedSeq,
              sponsorship,
              revisionsMap.get(version).flatten,
              collectionsMap.get(dataset, version).getOrElse(Nil).toIndexedSeq,
              externalPublicationsMap
                .get(dataset, version)
                .getOrElse(Nil)
                .toIndexedSeq
            )
        }.toList
      )
  }

  /**
    * Helper query to get all entities that compose a dataset version.
    */
  def getDatasetDetails(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIO[
    (
      List[PublicContributor],
      List[PublicCollection],
      List[PublicExternalPublication],
      Option[Sponsorship],
      Option[Revision]
    )
  ] =
    for {
      contributors <- PublicContributorsMapper
        .getContributorsByDatasetAndVersion(dataset, version)

      collections <- PublicCollectionsMapper
        .getCollectionsByDatasetAndVersion(dataset, version)

      externalPublications <- PublicExternalPublicationsMapper
        .getByDatasetAndVersion(dataset, version)

      sponsorship <- SponsorshipsMapper.maybeGetByDataset(dataset)

      revision <- RevisionsMapper.getLatestRevision(version)

    } yield
      (contributors, collections, externalPublications, sponsorship, revision)

  def getVersions(
    id: Int,
    status: Set[PublishStatus] = Set(PublishSucceeded)
  )(implicit
    executionContext: ExecutionContext
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    getVersions(status = status, ids = List(id))

  def getVersions(
    status: Set[PublishStatus],
    ids: List[Int]
  )(implicit
    executionContext: ExecutionContext
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    this
      .filter(_.datasetId inSet ids)
      .filter(_.status inSet status)
      .sortBy(_.version.desc)

  def setStatus(
    id: Int,
    version: Int,
    status: PublishStatus
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDatasetVersion,
    NoStream,
    Effect.Read with Effect.Write with Effect
  ] =
    for {
      v <- getVersion(id, version)
      result <- setStatus(id, List(v), status)
        .flatMap {
          _.headOption match {
            case None => DBIO.failed(NoDatasetVersionException(id, version))
            case Some(dataset) => DBIO.successful(dataset)
          }
        }
    } yield result

  def setStatus(
    id: Int,
    versions: List[PublicDatasetVersion],
    status: PublishStatus
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[List[PublicDatasetVersion], NoStream, Effect.Read with Effect.Write] = {

    val updatedVersions = versions.map(_.copy(status = status))
    for {
      _ <- this
        .filter(_.datasetId === id)
        .filter(_.version inSet versions.map(_.version))
        .map(_.status)
        .update(status)
    } yield updatedVersions
  }

  def setResultMetadata(
    version: PublicDatasetVersion,
    size: Long,
    fileCount: Long,
    readme: S3Key.File,
    banner: S3Key.File
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetVersion, NoStream, Effect.Read with Effect.Write] =
    for {
      _ <- this
        .filter(_.datasetId === version.datasetId)
        .filter(_.version === version.version)
        .update(
          version
            .copy(
              size = size,
              fileCount = fileCount,
              readme = Some(readme),
              banner = Some(banner)
            )
        )

      updated <- getVersion(version.datasetId, version.version)
    } yield updated

  def setS3Bucket(
    version: PublicDatasetVersion,
    s3Bucket: S3Bucket
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetVersion, NoStream, Effect.Read with Effect.Write] =
    for {
      _ <- PublicDatasetVersionsMapper
        .filter(_.datasetId === version.datasetId)
        .filter(_.version === version.version)
        .map(_.s3Bucket)
        .update(s3Bucket)

      updated <- getVersion(version.datasetId, version.version)
    } yield updated

  def setExecutionArn(
    version: PublicDatasetVersion,
    executionArn: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Unit, NoStream, Effect.Read with Effect.Write] =
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .map(_.executionArn)
      .update(Some(executionArn))
      .map(_ => ())

  def setReleaseExecutionArn(
    version: PublicDatasetVersion,
    releaseExecutionArn: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Unit, NoStream, Effect.Read with Effect.Write] =
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .map(_.releaseExecutionArn)
      .update(Some(releaseExecutionArn))
      .map(_ => ())

  def create(
    id: Int,
    size: Long = 0L,
    description: String,
    modelCount: Map[String, Long] = Map.empty,
    fileCount: Long = 0L,
    recordCount: Long = 0L,
    status: PublishStatus,
    doi: String,
    schemaVersion: PennsieveSchemaVersion,
    s3Bucket: S3Bucket,
    banner: Option[S3Key.File] = None,
    readme: Option[S3Key.File] = None,
    embargoReleaseDate: Option[LocalDate] = None
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicDatasetVersion,
    NoStream,
    Effect.Read with Effect with Effect.Write with Effect.Transactional
  ] = {
    val query: DBIOAction[
      PublicDatasetVersion,
      NoStream,
      Effect.Read with Effect with Effect.Write
    ] = for {
      datasets <- this.filter(_.datasetId === id).result
      latestVersion = if (datasets.isEmpty) {
        0
      } else {
        datasets.map(_.version).max
      }
      version = latestVersion + 1
      row = PublicDatasetVersion(
        datasetId = id,
        version = version,
        size = size,
        description = description,
        modelCount = modelCount,
        fileCount = fileCount,
        recordCount = recordCount,
        s3Bucket = s3Bucket,
        s3Key = S3Key.Version(id, version),
        status = status,
        doi = doi,
        schemaVersion = schemaVersion,
        banner = banner,
        readme = readme,
        embargoReleaseDate = embargoReleaseDate
      )
      result <- (this returning this) += row
    } yield result

    query.transactionally.asTry.flatMap {
      case Success(result) =>
        DBIO.successful(result)
      case Failure(exception: PSQLException) =>
        DBIO.failed(PublicDatasetVersionExceptionMapper.apply(exception))
      case Failure(exception: Throwable) =>
        DBIO.failed(exception)
    }
  }

  def rollbackIfNeeded(
    dataset: PublicDataset,
    onDelete: PublicDatasetVersion => Future[Unit] = _ => Future.successful(())
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Unit, NoStream, Effect.Read with Effect.Write] = {
    getLatestVersion(dataset.id)
      .flatMap {
        case Some(latestVersion) if latestVersion.underEmbargo =>
          deleteVersion(latestVersion)
            .flatMap(_ => DBIO.from(onDelete(latestVersion)))
        case Some(latestVersion) =>
          latestVersion.status match {
            case PublishFailed =>
              deleteVersion(latestVersion)
                .flatMap(_ => DBIO.from(onDelete(latestVersion)))
            case _ => DBIO.successful(())
          }
        case None => DBIO.successful(())
      }
  }

  def updateVersion(
    version: PublicDatasetVersion,
    description: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicDatasetVersion, NoStream, Effect.Write] = {
    val updated = version.copy(description = description)
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .update(updated)
      .map(_ => updated)
  }

  def deleteVersion(
    version: PublicDatasetVersion
  ): FixedSqlAction[Int, NoStream, Effect.Write] = {
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .delete
  }

  def isDuplicateDoi(
    doi: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Boolean, NoStream, Effect.Read] = {
    this.filter(_.doi === doi).exists.result
  }

  /**
    * Slick filter that can be applied to the dataset versions table
    *
    * The monoid instance is used to OR together an arbitrary set of filters, eg
    *
    *     (d.id === 1 && v.version === 5) || (d.id === 3 && v.version === 5)
    *
    * Using compound OR statements in Postgres is not as efficient as an IN, but
    * should do the job since the table is relatively small.
    */
  type DatasetVersionFilter =
    PublicDatasetVersionsTable => slick.lifted.Rep[Boolean]

  implicit val filterMonoid = new Monoid[DatasetVersionFilter] {
    def combine(
      f: DatasetVersionFilter,
      g: DatasetVersionFilter
    ): DatasetVersionFilter =
      d => f(d) || g(d)

    def empty: DatasetVersionFilter = d => false.bind
  }

  private def filterByVersion(
    version: PublicDatasetVersion
  ): DatasetVersionFilter =
    d =>
      (d.datasetId === version.datasetId.bind && d.version === version.version.bind)

  def getMany(
    versions: Seq[PublicDatasetVersion]
  ): Query[PublicDatasetVersionsTable, PublicDatasetVersion, Seq] =
    PublicDatasetVersionsMapper.filter(
      Monoid
        .combineAll(versions.map(filterByVersion))
    )

  def increaseDatasetDownloadCounter(
    dataset: PublicDataset,
    version: Int
  )(implicit
    executionContext: ExecutionContext
  ) =
    sql"""UPDATE public_dataset_versions
          SET dataset_downloads_counter = dataset_downloads_counter + 1
          WHERE dataset_id = ${dataset.id} AND version = ${version};
        """.as[Int]

  def increaseFilesDownloadCounter(
    dataset: PublicDataset,
    version: Int,
    numberOfFiles: Int
  )(implicit
    executionContext: ExecutionContext
  ) =
    sql"""UPDATE public_dataset_versions
          SET file_downloads_counter = file_downloads_counter + ${numberOfFiles}
          WHERE dataset_id = ${dataset.id} AND version = ${version};
        """.as[Int]

}

object PublicDatasetVersionExceptionMapper {

  def apply: Throwable => Throwable = {
    case exception: PSQLException => {
      val message: String = exception.getMessage()
      if (message
          .contains(
            """duplicate key value violates unique constraint "public_dataset_versions_doi_key""""
          )) {
        DuplicateDoiException
      } else {
        exception
      }
    }
    case throwable => throwable
  }

}
