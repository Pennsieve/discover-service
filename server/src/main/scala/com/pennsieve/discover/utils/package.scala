// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import java.time.temporal.ChronoUnit

import com.pennsieve.discover.models.DatasetDownload
import com.pennsieve.models._
import org.apache.commons.lang3.StringUtils
import scala.concurrent._

package object utils {

  /**
    * Join a path, removing intervening whitespace and leading slashes.
    */
  def joinPath(pathPrefix: String, pathSuffix: String): String =
    StringUtils.removeStart(
      stripSlashes(pathPrefix) + "/" +
        stripSlashes(pathSuffix),
      "/"
    )

  def joinPath(paths: String*): String =
    paths.fold("")(joinPath)

  def stripSlashes(s: String): String =
    s.replaceFirst("^[/]+", "").replaceFirst("[/]+$", "")

  /**
    * Convert a string to a Pennsieve FileType.
    */
  def getFileType(s: String): FileType =
    FileType
      .withNameInsensitiveOption(s)
      .getOrElse(FileType.GenericData)

  /**
    * Convert a file extension to a Pennsieve FileType.
    */
  def getFileTypeFromExtension(s: String): FileType = {
    val withDot = if (s.startsWith(".")) s else s".$s"
    FileExtensions.fileTypeMap.getOrElse(withDot, FileType.GenericData)
  }

  /**
    * Get the Pennsieve package type for a file type.
    *
    * The frontend uses the package type to display icons in the file browser.
    */
  def getPackageType(fileType: FileType): PackageType =
    FileTypeInfo.get(fileType).packageType

  def getIcon(fileType: FileType): Icon =
    FileTypeInfo
      .get(fileType)
      .icon

  /**
    *
    * @param athenaDownloads   : An array of DatasetDownload originating from Athena that may contains info already in the
    *                          database
    * @param databaseDownloads : An array of DatasetDownload originating from discover database
    * @return : An Array of DatasetDownload contains the downloads from both origins, once deduplicated
    */
  // Deduplication:
  // An Athena Download is considered a duplicate of a Database Download if at least one of those is true:
  // - Both Downloads have the same request ID
  // - Both Downloads are for the same dataset, same version within 2 seconds of each other
  def cleanAthenaDownloads(
    athenaDownloads: List[DatasetDownload],
    databaseDownloads: List[DatasetDownload]
  ): List[DatasetDownload] = {
    athenaDownloads.filter { athenaDL =>
      databaseDownloads.collectFirst {
        case databaseDL
            if (databaseDL.requestId == athenaDL.requestId) && (databaseDL.requestId.isDefined) ||
              ((databaseDL.datasetId == athenaDL.datasetId) && (databaseDL.version == athenaDL.version) && (
                athenaDL.downloadedAt
                  .until(databaseDL.downloadedAt, ChronoUnit.SECONDS)
                  .abs < 2
              )) =>
          databaseDL
      }.isEmpty
    }
  }

  /**
    * Executes asynchronous function on each item in the iterable sequentially (not in parallel)
    * @param items: the iterable of items
    * @param f: the asynchronous function that returns a Future
    * @param ec: ExecutionContext
    * @tparam T
    * @tparam U
    * @return Future with a List of Items
    * @reference: https://stackoverflow.com/questions/20414500/how-to-do-sequential-execution-of-futures-in-scala
    */
  def runSequentially[T, U](
    items: IterableOnce[T]
  )(
    f: T => Future[U]
  )(implicit
    ec: ExecutionContext
  ): Future[List[U]] = {
    items.iterator.foldLeft(Future.successful[List[U]](Nil)) { (acc, item) =>
      acc.flatMap { x =>
        f(item).map(_ :: x)
      }
    } map (_.reverse)
  }
}
