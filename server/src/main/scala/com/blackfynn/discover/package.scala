// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover

import java.time.temporal.ChronoUnit

import com.blackfynn.discover.models.{
  DatasetDownload,
  PublicDatasetVersion,
  S3Key
}
import com.blackfynn.models._
import org.apache.commons.lang3.StringUtils

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
}
