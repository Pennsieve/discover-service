// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import java.time.OffsetDateTime

import com.blackfynn.discover.utils
import com.blackfynn.models.FileType
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
    s3Version: Option[ObjectVersion],
    size: Long,
    sourcePackageId: Option[String],
    createdAt: Option[OffsetDateTime] = None,
    sourceFileId: Option[String] = None
  ) extends FileTreeNode

  case class Directory(name: String, path: String, size: Long)
      extends FileTreeNode

  def apply(file: PublicFile): FileTreeNode = {

    val path =
      if (file.s3Key.toString
          .startsWith("versioned/" + file.datasetId + "/")) {
        file.s3Key.toString
          .replace("versioned/" + file.datasetId + "/", "")
      } else {
        file.s3Key.toString
      }

    File(
      name = file.name,
      path = path,
      fileType = utils.getFileType(file.fileType),
      s3Key = file.s3Key,
      s3Version = file.s3Version,
      size = file.size,
      sourcePackageId = file.sourcePackageId,
      createdAt = Some(file.createdAt),
      sourceFileId = file.sourceFileId
    )
  }
}
