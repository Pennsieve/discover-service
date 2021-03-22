// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.db

import akka.Done
import cats.implicits._
import com.blackfynn.discover.NoFileException
import com.blackfynn.discover.db.profile.api._
import com.blackfynn.discover.models.{
  FileDownloadDTO,
  FileTreeNode,
  ObjectVersion,
  PublicDatasetVersion,
  PublicFile,
  S3Key
}
import com.blackfynn.discover.utils.{ getFileType, joinPath }
import com.blackfynn.models.{ FileManifest, PublishStatus }
import com.github.tminglei.slickpg._
import slick.basic.DatabasePublisher
import slick.jdbc.{ GetResult, ResultSetConcurrency, ResultSetType }

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.Base64
import scala.concurrent.ExecutionContext

final class PublicFilesTable(tag: Tag)
    extends Table[PublicFile](tag, "public_files") {

  def name = column[String]("name")
  def datasetId = column[Int]("dataset_id")
  def version = column[Int]("version")
  def fileType = column[String]("file_type")
  def size = column[Long]("size")
  // File path in S3
  def s3Key = column[S3Key.File]("s3_key")
  def s3Version = column[Option[ObjectVersion]]("s3_version")
  // LTree representation of S3 path
  def path = column[LTree]("path")
  def sourcePackageId = column[Option[String]]("source_package_id")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def sourceFileId = column[Option[String]]("source_file_id")

  def publicDatasetVersion =
    foreignKey(
      "public_dataset_versions_fk",
      (datasetId, version),
      PublicDatasetVersionsMapper
    )(v => (v.datasetId, v.version))

  def * =
    (
      name,
      datasetId,
      version,
      fileType,
      size,
      s3Key,
      path,
      sourcePackageId,
      createdAt,
      updatedAt,
      id,
      s3Version,
      sourceFileId
    ).mapTo[PublicFile]
}

object PublicFilesMapper extends TableQuery(new PublicFilesTable(_)) {

  /**
    * LTrees can only contain ASCII characters, and levels of the tree are
    * separated by `.` characters. This helper encodes directories and filenames
    * into base 64.
    */
  private def convertPathToTree(key: S3Key.File): String = {
    key.value
      .split("/")
      .map(_.getBytes(StandardCharsets.UTF_8))
      .map(Base64.getEncoder().withoutPadding().encodeToString(_))
      .mkString(".")
  }

  private def buildFile(
    version: PublicDatasetVersion,
    name: String,
    fileType: String,
    size: Long,
    s3Key: S3Key.File,
    s3Version: Option[ObjectVersion],
    sourcePackageId: Option[String],
    sourceFileId: Option[String]
  ): PublicFile =
    PublicFile(
      name = name,
      datasetId = version.datasetId,
      version = version.version,
      fileType = fileType,
      size = size,
      s3Key = s3Key,
      s3Version = s3Version,
      path = LTree(PublicFilesMapper.convertPathToTree(s3Key)),
      sourcePackageId = sourcePackageId,
      sourceFileId = sourceFileId
    )

  private def buildFile(
    version: PublicDatasetVersion,
    fileManifest: FileManifest
  ): PublicFile =
    buildFile(
      version = version,
      name = fileManifest.name,
      fileType = fileManifest.fileType.toString,
      size = fileManifest.size,
      s3Key = version.s3Key / fileManifest.path,
      s3Version = fileManifest.versionId.map(ObjectVersion(_)),
      sourcePackageId = fileManifest.sourcePackageId,
      sourceFileId = fileManifest.sourceFileId.map(_.toString)
    )

  def forVersion(
    version: PublicDatasetVersion
  ): Query[PublicFilesTable, PublicFile, Seq] =
    this
      .filter(_.version === version.version)
      .filter(_.datasetId === version.datasetId)

  /**
    * Create a reactive publisher for all files belonging to this version.
    * This can be converted to a stream with `Source.fromPublisher`
    *
    * The extra statement parameters are needed properly stream from Postgres.
    * See https://scala-slick.org/doc/3.2.3/dbio.html#streaming
    */
  def streamForVersion(
    version: PublicDatasetVersion,
    db: Database
  ): DatabasePublisher[PublicFile] =
    db.stream(
      PublicFilesMapper
        .forVersion(version)
        .result
        .withStatementParameters(
          rsType = ResultSetType.ForwardOnly,
          rsConcurrency = ResultSetConcurrency.ReadOnly,
          fetchSize = 1000
        )
        .transactionally
    )

  def create(
    version: PublicDatasetVersion,
    name: String,
    fileType: String,
    size: Long,
    s3Key: S3Key.File,
    sourcePackageId: Option[String] = None,
    s3Version: Option[ObjectVersion] = None,
    sourceFileId: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicFile,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    (this returning this) += buildFile(
      version = version,
      name = name,
      fileType = fileType,
      size = size,
      s3Key = s3Key,
      s3Version = s3Version,
      sourcePackageId = sourcePackageId,
      sourceFileId = sourceFileId
    )

  def getFile(
    version: PublicDatasetVersion,
    path: S3Key.File
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[FileTreeNode, NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === version.datasetId)
      .filter(_.version === version.version)
      .filter(_.s3Key === path)
      .result
      .headOption
      .flatMap {
        case Some(f) =>
          DBIO.successful(FileTreeNode(f))
        case _ =>
          DBIO.failed(NoFileException(version.datasetId, version.version, path))
      }

  def getFileFromSourcePackageId(
    sourcePackageId: String,
    limit: Int,
    offset: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[(Long, List[PublicFile]), NoStream, Effect.Read with Effect] = {
    val latestDatasetVersions =
      PublicDatasetVersionsMapper.getLatestDatasetVersions(
        PublishStatus.PublishSucceeded
      )

    val allMatchingFiles = this
      .join(latestDatasetVersions)
      .on {
        case (file, dataset) =>
          file.datasetId === dataset.datasetId && file.version === dataset.version
      }
      .filter(_._1.sourcePackageId === sourcePackageId)

    for {
      totalCount <- allMatchingFiles.length.result.map(_.toLong)

      files <- allMatchingFiles
        .sortBy(_._1.name)
        .drop(offset)
        .take(limit)
        .map(_._1)
        .result
        .map(_.toList)

    } yield (totalCount, files)
  }

  def createMany(
    version: PublicDatasetVersion,
    files: List[FileManifest]
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Done,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] =
    DBIO
      .sequence(
        files
          .map(buildFile(version, _))
          .grouped(10000)
          .map(this ++= _)
          .toList
      )
      .map(_ => Done)
      .transactionally

  def getFileDownloadsMatchingPaths(
    version: PublicDatasetVersion,
    paths: Seq[String]
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[Seq[FileDownloadDTO], NoStream, Effect.Read with Effect] = {

    val treePaths =
      paths
        .map(p => s"${convertPathToTree(version.s3Key / p)}.*")

    implicit val fileDownloadGetter: GetResult[FileDownloadDTO] = GetResult(
      r => {
        val name = r.nextString
        val s3Key = S3Key.File(r.nextString)
        val size = r.nextLong
        FileDownloadDTO(version, name, s3Key, size)
      }
    )

    sql"""
            SELECT
              name, s3_key, size
            FROM
              public_files as f
            WHERE f.path ?? $treePaths::lquery[]
        """.as[FileDownloadDTO]
  }

  case class TotalCount(value: Long) extends AnyVal

  /**
    * Find the files and directories under a given parent node.
    *
    * Computing files is easy, since they are already represented in the
    * database. On the other hand, directories are implicit, and found by
    * looking at all files with the parent prefix then slicing the file path to
    * get the directory name at the correct level in the tree.
    *
    * In order to compute the total size of the result set in a single query
    * along with the file rows, the query adds a row containing the total count.
    *
    * Note: this does not filter on the dataset/version foreign key, since that
    * constraint is implicitly defined by the file path. In addition Postgres
    * does not have a joint index on id, version, and tree so there might be
    * some performance issues with that. This will need to be updated when S3
    * versioning is enabled.
    */
  def childrenOf(
    version: PublicDatasetVersion,
    path: Option[String],
    limit: Int = 100,
    offset: Int = 0
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    (TotalCount, Seq[FileTreeNode]),
    NoStream,
    Effect.Read with Effect
  ] = {

    implicit val getTreeNodeResult
      : GetResult[Either[TotalCount, FileTreeNode]] =
      buildTreeNodeGetter(path)

    val parent = convertPathToTree(path match {
      case Some(d) => version.s3Key / d
      case None => version.s3Key / ""
    })

    // selects leaves of the tree one level under parent
    val leafChildSelector = parent + ".*{1}"

    val result = sql"""
     WITH
       files AS (
         SELECT f.name, f.file_type, f.s3_key, f.size, f.source_package_id, f.s3_version, f.source_file_id
         FROM public_files AS f
         WHERE f.path ~ $leafChildSelector::lquery
       ),

       directories AS (
         SELECT q.name, q.file_type, q.s3_key, sum(q.size) as size, q.source_package_id, q.s3_version, q.source_file_id
         FROM (
           SELECT
             split_part(f.s3_key, '/', nlevel($parent::ltree) + 1) AS name,
             null::text AS file_type,
             null::text AS s3_key,
             size,
             null::text AS source_package_id,
             null::text AS s3_version,
             null::text AS source_file_id
           FROM public_files AS f
           WHERE NOT f.path ~ $leafChildSelector::lquery
           AND f.path <@ $parent::ltree
           AND nlevel(f.path) > nlevel($parent::ltree)
         ) as q
         GROUP BY q.name, q.file_type, q.s3_key, q.source_package_id, q.s3_version, q.source_file_id
       ),

       -- Compute the total number of file + directory results, stored in the `size` column
       total_count AS (
         SELECT
           null::text AS name,
           null::text AS file_type,
           null::text AS s3_key,
           (SELECT COALESCE(COUNT(*), 0) FROM files) + (SELECT COALESCE(COUNT(*), 0) FROM directories) AS size,
           null::text AS source_package_id,
           null::text AS s3_version,
           null::text AS source_file_id
       )
     (
       SELECT 'count', * FROM total_count
       UNION (
         (
           SELECT 'directory', * FROM directories
           UNION
           SELECT 'file', * FROM files
         )
         ORDER BY 1 ASC, 2 ASC  -- Folders first, then files, ordered by name
         LIMIT $limit
         OFFSET $offset
       )
     )
     ORDER BY 1 ASC, 2 ASC  -- Ensure rows are still ordered after the `count` union
    """.as[Either[TotalCount, FileTreeNode]]

    // Separate the header row containing the total number of rows from the rows
    // of file nodes
    for {
      rows <- result
      (counts, nodes) = rows.separate
      totalCount <- counts.headOption
        .map(DBIO.successful(_))
        .getOrElse(DBIO.failed(new Exception("missing 'count'")))
    } yield (totalCount, nodes)
  }

  /**
    * Unwrap SQL row into a FileTreeNode
    *
    * This builder takes a base path that is used to construct the full path
    * (relative to the dataset root) of the files and directories in the query.
    */
  private def buildTreeNodeGetter(
    basePath: Option[String]
  ): GetResult[Either[TotalCount, FileTreeNode]] =
    GetResult(r => {
      val nodeType = r.nextString

      nodeType match {
        case "count" => Left(TotalCount(r.skip.skip.skip.nextLong))
        case "file" =>
          val name = r.nextString
          Right(
            FileTreeNode
              .File(
                name = name,
                path = buildPath(basePath, name),
                fileType = getFileType(r.nextString),
                s3Key = S3Key.File(r.nextString),
                size = r.nextLong,
                sourcePackageId = r.nextStringOption,
                s3Version = r.nextStringOption.map(ObjectVersion(_)),
                createdAt = None,
                sourceFileId = r.nextStringOption
              )
          )
        case "directory" =>
          val name = r.nextString
          r.skip
          r.skip
          val size = r.nextLong
          Right(
            FileTreeNode
              .Directory(name = name, path = buildPath(basePath, name), size)
          )
      }
    })

  /**
    * Join a base path to a filename
    */
  private def buildPath(basePath: Option[String], name: String): String =
    basePath.map(joinPath(_, name)) getOrElse name
}
