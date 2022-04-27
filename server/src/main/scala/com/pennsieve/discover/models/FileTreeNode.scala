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
    createdAt: Option[OffsetDateTime] = None
  ) extends FileTreeNode

  case class Directory(name: String, path: String, size: Long)
      extends FileTreeNode

  def apply(file: PublicFile, s3Bucket: S3Bucket): FileTreeNode = {

    val path =
      if (file.s3Key.toString
          .startsWith(file.datasetId + "/" + file.version)) {
        file.s3Key.toString
          .replace(file.datasetId + "/" + file.version + "/", "")
      } else {
        file.s3Key.toString
      }

    File(
      file.name,
      path,
      utils.getFileType(file.fileType),
      file.s3Key,
      s3Bucket,
      file.size,
      file.sourcePackageId,
      Some(file.createdAt)
    )
  }
}
