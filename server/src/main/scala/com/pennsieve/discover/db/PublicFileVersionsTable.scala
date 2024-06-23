// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.Done
import cats.implicits._
import com.github.tminglei.slickpg._
import com.pennsieve.discover.{ NoFileException, NoFileVersionException }

import java.util.UUID
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  FileDownloadDTO,
  FileTreeNode,
  PublicDatasetVersion,
  PublicDatasetVersionFile,
  PublicDatasetVersionFileVersion,
  PublicFile,
  PublicFileVersion,
  ReleaseAction,
  S3Bucket,
  S3Key
}
import com.pennsieve.discover.utils.{ getFileType, joinPath }
import com.pennsieve.models.{ FileManifest, PublishStatus }
import slick.basic.DatabasePublisher
import slick.dbio.{ DBIOAction, Effect }
import slick.jdbc.{ GetResult, ResultSetConcurrency, ResultSetType }

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success }

final class PublicFileVersionsTable(tag: Tag)
    extends Table[PublicFileVersion](tag, "public_file_versions") {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def name = column[String]("name")
  def fileType = column[String]("file_type")
  def size = column[Long]("size")
  def sourcePackageId = column[Option[String]]("source_package_id")
  def sourceFileUUID = column[Option[UUID]]("source_file_uuid")
  def s3Key = column[S3Key.File]("s3_key")
  def s3Version = column[String]("s3_version")
  def path = column[LTree]("path")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")
  def datasetId = column[Int]("dataset_id")
  def sha256 = column[Option[String]]("sha256")

  def * =
    (
      id,
      name,
      fileType,
      size,
      sourcePackageId,
      sourceFileUUID,
      s3Key,
      s3Version,
      path,
      createdAt,
      updatedAt,
      datasetId,
      sha256
    ).mapTo[PublicFileVersion]
}

object PublicFileVersionsMapper
    extends TableQuery(new PublicFileVersionsTable(_)) {

  private def datasetVersionFiles(version: PublicDatasetVersion) =
    PublicDatasetVersionFilesTableMapper
      .filter(_.datasetId === version.datasetId)
      .filter(_.datasetVersion === version.version)

  /**
    * LTrees can only contain ASCII characters, and levels of the tree are
    * separated by `.` characters. This helper encodes directories and filenames
    * into base 64.
    */
  def convertPathToTree(key: S3Key.File): String = {
    key.value
      .split("/")
      .map(_.getBytes(StandardCharsets.UTF_8))
      .map(pgSafeBase64)
      .mkString(".")
  }

  // TODO: return PublicFileVersion? (or "unified" PublicFile)
  private def buildFile(
    version: PublicDatasetVersion,
    name: String,
    fileType: String,
    size: Long,
    s3Key: S3Key.File,
    sourcePackageId: Option[String]
  ): PublicFile =
    PublicFile(
      name = name,
      datasetId = version.datasetId,
      version = version.version,
      fileType = fileType,
      size = size,
      s3Key = s3Key,
      path = LTree(PublicFileVersionsMapper.convertPathToTree(s3Key)),
      sourcePackageId = sourcePackageId
    )

  // TODO: return PublicFileVersion? (or "unified" PublicFile)
  private def buildFile(
    version: PublicDatasetVersion,
    fileManifest: FileManifest
  ): PublicFile =
    PublicFileVersionsMapper.buildFile(
      version = version,
      name = fileManifest.name,
      fileType = fileManifest.fileType.toString,
      size = fileManifest.size,
      s3Key = version.s3Key / fileManifest.path,
      sourcePackageId = fileManifest.sourcePackageId
    )

  private def buildFileVersion(
    datasetId: Int,
    name: String,
    fileType: String,
    size: Long,
    s3Key: S3Key.File,
    s3Version: String,
    sourcePackageId: Option[String],
    sourceFileUUID: Option[UUID],
    sha256: Option[String]
  ): PublicFileVersion =
    PublicFileVersion(
      name = name,
      fileType = fileType,
      size = size,
      sourcePackageId = sourcePackageId,
      sourceFileUUID = sourceFileUUID,
      s3Key = s3Key,
      s3Version = s3Version,
      path = LTree(PublicFileVersionsMapper.convertPathToTree(s3Key)),
      datasetId = datasetId,
      sha256 = sha256
    )

  private def buildFileVersion(
    version: PublicDatasetVersion,
    fileManifest: FileManifest
  ): PublicFileVersion =
    buildFileVersion(
      datasetId = version.datasetId,
      name = fileManifest.name,
      fileType = fileManifest.fileType.toString,
      size = fileManifest.size,
      s3Key = version.s3Key / fileManifest.path,
      s3Version = fileManifest.s3VersionId.getOrElse("missing"),
      sourcePackageId = fileManifest.sourcePackageId,
      sourceFileUUID = fileManifest.id,
      sha256 = fileManifest.sha256
    )

  def forVersion(
    version: PublicDatasetVersion
  ): Query[PublicFileVersionsTable, PublicFileVersion, Seq] =
    this
      .join(datasetVersionFiles(version))
      .on(_.id === _.fileId)
      .map(_._1)

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
  ): DatabasePublisher[PublicFileVersion] =
    db.stream(
      PublicFileVersionsMapper
        .forVersion(version)
        .result
        .withStatementParameters(
          rsType = ResultSetType.ForwardOnly,
          rsConcurrency = ResultSetConcurrency.ReadOnly,
          fetchSize = 1000
        )
        .transactionally
    )

//  def create(
//    version: PublicDatasetVersion,
//    name: String,
//    fileType: String,
//    size: Long,
//    s3Key: S3Key.File,
//    sourcePackageId: Option[String] = None
//  )(implicit
//    executionContext: ExecutionContext
//  ): DBIOAction[
//    PublicFile,
//    NoStream,
//    Effect.Read with Effect.Write with Effect.Transactional with Effect
//  ] =
//    (this returning this) += buildFile(
//      version = version,
//      name = name,
//      fileType = fileType,
//      size = size,
//      s3Key = s3Key,
//      sourcePackageId = sourcePackageId
//    )

  def getFile(
    version: PublicDatasetVersion,
    path: S3Key.File
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[FileTreeNode, NoStream, Effect.Read with Effect] = {
    val datasetVersionFiles =
      PublicDatasetVersionFilesTableMapper
        .filter(_.datasetId === version.datasetId)
        .filter(_.datasetVersion === version.version)

    this
      .join(datasetVersionFiles)
      .on(_.id === _.fileId)
      .filter(_._1.s3Key === path)
      .result
      .headOption
      .flatMap {
        case Some((f, _)) =>
          DBIO.successful(FileTreeNode(f, version))
        case _ =>
          DBIO.failed(NoFileException(version.datasetId, version.version, path))
      }
  }

  def getFileForVersion(
    version: PublicDatasetVersion,
    path: S3Key.File
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicFileVersion, NoStream, Effect.Read with Effect] = {
    val datasetVersionFiles =
      PublicDatasetVersionFilesTableMapper
        .filter(_.datasetId === version.datasetId)
        .filter(_.datasetVersion === version.version)

    this
      .join(datasetVersionFiles)
      .on(_.id === _.fileId)
      .filter(_._1.s3Key === path)
      .result
      .headOption
      .flatMap {
        case Some((f, _)) =>
          DBIO.successful(f)
        case _ =>
          DBIO.failed(NoFileException(version.datasetId, version.version, path))
      }
  }
  def getFileVersion(
    datasetId: Int,
    s3Key: S3Key.File,
    s3Version: String
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[PublicFileVersion, NoStream, Effect.Read with Effect] =
    this
      .filter(_.datasetId === datasetId)
      .filter(_.s3Key === s3Key)
      .filter(_.s3Version === s3Version)
      .result
      .headOption
      .flatMap {
        case None =>
          DBIO.failed(NoFileVersionException(datasetId, s3Key, s3Version))
        case Some(fileVersion) =>
          DBIO.successful((fileVersion))
      }

  def getFileFromSourcePackageId(
    sourcePackageId: String,
    limit: Int,
    offset: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    (Long, Option[Int], List[(PublicFileVersion, S3Bucket)]),
    NoStream,
    Effect.Read with Effect
  ] = {
    val latestDatasetVersions =
      PublicDatasetVersionsMapper.getLatestDatasetVersions(
        PublishStatus.PublishSucceeded
      )

    val allMatchingFiles = this
      .join(PublicDatasetVersionFilesTableMapper)
      .join(latestDatasetVersions)
      .join(PublicDatasetsMapper)
      .on {
        case (((file, joinTable), version), dataset) =>
          file.id === joinTable.fileId &&
            joinTable.datasetId === version.datasetId &&
            joinTable.datasetVersion === version.version &&
            dataset.id === version.datasetId
      }
      .filter(_._1._1._1.sourcePackageId === sourcePackageId)

    for {
      totalCount <- allMatchingFiles.length.result.map(_.toLong)

      // These should all be the same
      organizationId <- allMatchingFiles
        .map(_._2.sourceOrganizationId)
        .result
        .headOption

      files <- allMatchingFiles
        .sortBy(_._1._1._1.name)
        .drop(offset)
        .take(limit)
        .map(x => (x._1._1._1, x._1._2.s3Bucket))
        .result
        .map(_.toList)

    } yield (totalCount, organizationId, files)
  }

  def getAll(
    datasetId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Seq[PublicFileVersion], NoStream, Effect.Read with Effect] = {
    val query =
      this
        .filter(_.datasetId === datasetId)

    for {
      allFileVersions <- query.result
        .map(_.toList)
    } yield (allFileVersions)
  }

  //  def forVersion(
  //                  version: PublicDatasetVersion
  //                ): Query[PublicFileVersionsTable, PublicFile, Seq] =
  //    this
  //      .filter(_.version === version.version)
  //      .filter(_.datasetId === version.datasetId)

  def create(
    fileVersion: PublicFileVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] = (this returning this) += fileVersion

  def createOne(
    version: PublicDatasetVersion,
    file: FileManifest
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] = (this returning this) += buildFileVersion(version, file)

  def createAndLink(
    version: PublicDatasetVersion,
    file: FileManifest
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] =
    for {
      fileVersion <- createOne(version, file)
      _ <- PublicDatasetVersionFilesTableMapper.storeLinks(
        version,
        List(fileVersion)
      )
    } yield fileVersion

  def createAndLinkMany(
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
          .map(file => createAndLink(version, file))
      )
      .map(_ => Done)
      .transactionally

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
          .map(buildFileVersion(version, _))
          .grouped(10000)
          .map((this returning this) ++= _)
          .toList
      )
      .map(_ => Done)
      .transactionally

  def findOrCreate(
    version: PublicDatasetVersion,
    file: FileManifest
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] = {
    def get = getFileVersion(
      version.datasetId,
      version.s3Key / file.path,
      file.s3VersionId.getOrElse("missing")
    )

    def add = createOne(version, file)

    get.asTry.flatMap {
      case Failure(_: NoFileVersionException) => add
      case Failure(e) => DBIO.failed(e)
      case Success(s) => DBIO.successful(s)
    }.transactionally
  }

  def insert(
    pfv: PublicFileVersion
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    Int,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional
  ] = {
    val action = (
      this
        .filter(_.datasetId === pfv.datasetId)
        .filter(_.s3Key === pfv.s3Key)
        .filter(_.s3Version === pfv.s3Version)
        .result
        .headOption
        .flatMap {
          case Some(fileVersion) => DBIO.successful(fileVersion.id)
          case None =>
            (this returning this.map(_.id)) +=
              PublicFileVersion(
                name = pfv.name,
                fileType = pfv.fileType,
                size = pfv.size,
                sourcePackageId = pfv.sourcePackageId,
                sourceFileUUID = pfv.sourceFileUUID,
                s3Key = pfv.s3Key,
                s3Version = pfv.s3Version,
                path = pfv.path,
                datasetId = pfv.datasetId,
                sha256 = pfv.sha256
              )
//            val item = id.map {
//              id =>
//                PublicFileVersion(
//                  id = id,
//                  name = pfv.name,
//                  fileType = pfv.fileType,
//                  size = pfv.size,
//                  sourcePackageId = pfv.sourcePackageId,
//                  sourceFileUUID = pfv.sourceFileUUID,
//                  s3Key = pfv.s3Key,
//                  s3Version = pfv.s3Version,
//                  path = pfv.path,
//                  datasetId = pfv.datasetId,
//                  sha256 = pfv.sha256
//                )
//            }
//            item
        }
      )
      .transactionally
    action
  }

//  def connect(
//    versionAndFile: PublicDatasetVersionFileVersion
//  )(implicit
//    executionContext: ExecutionContext
//  ): DBIOAction[
//    PublicFileVersion,
//    NoStream,
//    Effect.Read with Effect.Write with Effect.Transactional
//  ] = {
//    val pfv = versionAndFile.file
//    val version = versionAndFile.version
//    val action = (
//      this
//        .filter(_.datasetId === pfv.datasetId)
//        .filter(_.s3Key === pfv.s3Key)
//        .filter(_.s3Version === pfv.s3Version)
//        .result
//        .headOption
//        .flatMap {
//          case Some(fileVersion) =>
//            DBIO.successful(fileVersion)
////            val item = (PublicDatasetVersionFilesTableMapper returning PublicDatasetVersionFilesTableMapper) += PublicDatasetVersionFile(
////              version.datasetId,
////              version.version,
////              fileVersion.id
////            )
////            DBIO.successful(item.map(item => item.fileId))
//          case None =>
//            val item = (this returning this) +=
//              PublicFileVersion(
//                name = pfv.name,
//                fileType = pfv.fileType,
//                size = pfv.size,
//                sourcePackageId = pfv.sourcePackageId,
//                sourceFileUUID = pfv.sourceFileUUID,
//                s3Key = pfv.s3Key,
//                s3Version = pfv.s3Version,
//                path = pfv.path,
//                datasetId = pfv.datasetId,
//                sha256 = pfv.sha256
//              )
//            item.map(item => DBIO.successful(item))
////            val fileId = id.
////            val item = id.map(
////              id =>
////                (PublicDatasetVersionFilesTableMapper returning PublicDatasetVersionFilesTableMapper) += PublicDatasetVersionFile(
////                  version.datasetId,
////                  version.version,
////                  id
////                )
////            )
////            DBIO.successful(item.map(item => item.map(item => item.fileId)))
//        }
//      )
//      .transactionally
//    action
//  }

//  def insertIfNotExists(productInput: ProductInput): Future[DBProduct] = {
//
//    val productAction = (
//      products.filter(_.uuid===productInput.uuid).result.headOption.flatMap {
//        case Some(product) =>
//          mylog("product was there: " + product)
//          DBIO.successful(product)
//
//        case None =>
//          mylog("inserting product")
//
//          val productId =
//            (products returning products.map(_.id)) += DBProduct(
//              0,
//              productInput.uuid,
//              productInput.name,
//              productInput.price
//            )
//
//          val product = productId.map { id => DBProduct(
//            id,
//            productInput.uuid,
//            productInput.name,
//            productInput.price
//          )
//          }
//          product
//      }
//      ).transactionally
//
//    db.run(productAction)
//  }

  def setS3Version(
    fileVersion: PublicFileVersion,
    s3Version: String
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Write with Effect.Transactional with Effect
  ] = {
    val updated = fileVersion.copy(s3Version = s3Version)
    this
      .filter(_.id === fileVersion.id)
      .update(updated)
      .map(_ => updated)
  }

  def updateS3Version(
    version: PublicDatasetVersion,
    action: ReleaseAction
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[
    PublicFileVersion,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    for {
      fileVersion <- getFileForVersion(version, S3Key.File(action.sourceKey))
      updated <- setS3Version(fileVersion, action.targetVersion)
    } yield updated

  def updateManyS3Versions(
    version: PublicDatasetVersion,
    actions: List[ReleaseAction]
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[
    Done,
    NoStream,
    Effect.Read with Effect.Write with Effect.Transactional with Effect
  ] =
    DBIO
      .sequence(
        actions
          .map(action => updateS3Version(version, action))
      )
      .map(_ => Done)
      .transactionally

  def getFileDownloadsMatchingPaths(
    version: PublicDatasetVersion,
    paths: Seq[String]
  )(implicit
    ec: ExecutionContext
  ): DBIOAction[Seq[FileDownloadDTO], NoStream, Effect.Read with Effect] = {

    // Assumes that the provided name is equal to the s3key file name
    val treePaths =
      paths
        .map(p => s"${convertPathToTree(version.s3Key / p)}.*")

    implicit val fileDownloadGetter: GetResult[FileDownloadDTO] = GetResult(
      r => {
        val name = r.nextString()
        val s3Key = S3Key.File(r.nextString())
        val size = r.nextLong()
        val s3Version = r.nextString()
        FileDownloadDTO(version, name, s3Key, size, Some(s3Version))
      }
    )
    val datasetId = version.datasetId
    val datasetVersion = version.version

    sql"""
          select pfv.name,
                 pfv.s3_key,
                 pfv.size,
                 pfv.s3_version,
                 pfv.sha256
          from discover.public_file_versions pfv
          join discover.public_dataset_version_files pdvf on (pdvf.file_id = pfv.id)
          where pdvf.dataset_id = $datasetId
            and pdvf.dataset_version = $datasetVersion
            and pfv.path ?? $treePaths::lquery[]
        """.as[FileDownloadDTO]
  }

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
      buildTreeNodeGetter(path, version.s3Bucket)

    val parent = convertPathToTree(path match {
      case Some(d) => version.s3Key / d
      case None => version.s3Key / ""
    })

    // selects leaves of the tree one level under parent
    val leafChildSelector = parent + ".*{1}"

    val datasetId = version.datasetId
    val datasetVersion = version.version

    val result = sql"""
     WITH
       files AS (
         SELECT name, file_type, s3_key, size, f.source_package_id, s3_version, sha256
         FROM public_file_versions AS f
         JOIN public_dataset_version_files v ON v.file_id = f.id
         WHERE f.path ~ $leafChildSelector::lquery
           AND v.dataset_id = $datasetId
           AND v.dataset_version = $datasetVersion
       ),

       directories AS (
         SELECT q.name, q.file_type, q.s3_key, sum(q.size) as size, q.source_package_id, q.s3_version, q.sha256
         FROM (
           SELECT
             split_part(f.s3_key, '/', nlevel($parent::ltree) + 1) AS name,
             null::text AS file_type,
             null::text AS s3_key,
             size,
             null::text AS source_package_id,
             null::text AS s3_version,
             null::text AS sha256
           FROM public_file_versions AS f
           JOIN public_dataset_version_files v ON v.file_id = f.id
           WHERE NOT f.path ~ $leafChildSelector::lquery
             AND f.path <@ $parent::ltree
             AND nlevel(f.path) > nlevel($parent::ltree)
             AND v.dataset_id = $datasetId
             AND v.dataset_version = $datasetVersion
         ) as q
         GROUP BY q.name, q.file_type, q.s3_key, q.source_package_id, s3_version, q.sha256
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
           null::text AS sha256
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
     ORDER BY 1 ASC, 2 ASC  -- Ensure rows are stil ordered after the `count` union
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
    basePath: Option[String],
    bucket: S3Bucket
  ): GetResult[Either[TotalCount, FileTreeNode]] =
    GetResult(r => {
      val nodeType = r.nextString()

      nodeType match {
        case "count" => Left(TotalCount(r.skip.skip.skip.nextLong()))
        case "file" =>
          val name = r.nextString()
          Right(
            FileTreeNode
              .File(
                name = name,
                path = buildPath(basePath, name),
                fileType = getFileType(r.nextString()),
                s3Key = S3Key.File(r.nextString()),
                s3Bucket = bucket,
                size = r.nextLong(),
                sourcePackageId = r.nextStringOption(),
                s3Version = r.nextStringOption(),
                sha256 = r.nextStringOption()
              )
          )
        case "directory" =>
          val name = r.nextString()
          r.skip
          r.skip
          val size = r.nextLong()
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
