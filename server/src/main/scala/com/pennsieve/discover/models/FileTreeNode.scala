// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.OffsetDateTime

import com.pennsieve.discover.utils
import com.pennsieve.models.FileType
import scala.collection.immutable

/**
  * A node in the Discover file hierarchy. This is either a concrete file, or a
  * directory that is implicitly defined by the files nested under it.
  */
sealed trait FileTreeNode

object FileTreeNode {

  case class File(
    name: String,
    path: String,
    fileType: FileType,
    s3Key: S3Key.File,
    s3Bucket: S3Bucket,
    size: Long,
    sourcePackageId: Option[String],
    createdAt: Option[OffsetDateTime] = None,
    s3Version: Option[String] = None,
    sha256: Option[String] = None
  ) extends FileTreeNode

  case class Directory(name: String, path: String, size: Long)
      extends FileTreeNode

  def trimPath(s3Key: S3Key.File, datasetId: Int, version: Int): String =
    if (s3Key.toString
        .startsWith(s"${datasetId}/${version}")) {
      s3Key.toString
        .replace(s"${datasetId}/${version}/", "")
    } else {
      s3Key.toString
    }

  def trimPath(s3Key: S3Key.File, datasetId: Int): String =
    if (s3Key.toString
        .startsWith(s"${datasetId}")) {
      s3Key.toString
        .replace(s"${datasetId}/", "")
    } else {
      s3Key.toString
    }

  def apply(file: PublicFile, s3Bucket: S3Bucket): FileTreeNode = {
    File(
      file.name,
      trimPath(file.s3Key, file.datasetId, file.version),
      utils.getFileType(file.fileType),
      file.s3Key,
      s3Bucket,
      file.size,
      file.sourcePackageId,
      Some(file.createdAt),
      s3Version = None,
      sha256 = None
    )
  }

  def apply(file: PublicFileVersion, s3Bucket: S3Bucket): FileTreeNode = {
    File(
      file.name,
      trimPath(file.s3Key, file.datasetId),
      utils.getFileType(file.fileType),
      file.s3Key,
      s3Bucket,
      file.size,
      file.sourcePackageId,
      Some(file.createdAt),
      s3Version = Some(file.s3Version),
      sha256 = file.sha256
    )
  }

  def apply(
    file: PublicFileVersion,
    version: PublicDatasetVersion
  ): FileTreeNode = {
    File(
      file.name,
      trimPath(file.s3Key, version.datasetId),
      utils.getFileType(file.fileType),
      file.s3Key,
      version.s3Bucket,
      file.size,
      file.sourcePackageId,
      Some(file.createdAt),
      s3Version = Some(file.s3Version),
      sha256 = file.sha256
    )
  }
}
