// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.clients.S3StreamClient
import com.pennsieve.discover.server.definitions.DownloadResponseItem

case class FileDownloadDTO(
  name: String,
  path: Seq[String],
  s3Bucket: S3Bucket,
  s3Key: S3Key.File,
  size: Long
) {
  def toDownloadResponseItem(
    s3Client: S3StreamClient,
    rootPath: Option[String]
  ): DownloadResponseItem = {
    DownloadResponseItem(
      name,
      rootPath
        .map(rp => {
          truncatePath(rp.split("/"), path)
        })
        .getOrElse(path)
        .toVector,
      s3Client.getPresignedUrlForFile(s3Bucket, s3Key),
      size
    )
  }

  /**
    * remove the rootPath portion of the path by evaluating the corresponding in order until
    * the result is emtpy or the tokens don't match
    * @param rootPath
    * @param path
    * @return a truncated path without the rootPath portion
    */
  def truncatePath(rootPath: Seq[String], path: Seq[String]): Seq[String] = {
    if (rootPath.isEmpty ||
      path.isEmpty ||
      rootPath.head != path.head) path
    else {
      truncatePath(rootPath.tail, path.tail)
    }
  }
}

case object FileDownloadDTO {
  def apply(
    version: PublicDatasetVersion,
    name: String,
    s3Key: S3Key.File,
    size: Long
  ): FileDownloadDTO = {
    FileDownloadDTO(
      name,
      getFilePath(version, s3Key),
      version.s3Bucket,
      s3Key,
      size
    )
  }

  /**
    * removes the dataset/version prefix as well as the filename itself from the path
    * @param version
    * @param file
    * @return list of strings representing the file's path within the version
    */
  def getFilePath(
    version: PublicDatasetVersion,
    file: S3Key.File
  ): Seq[String] = {
    file.removeVersionPrefix(version.s3Key).split("/").dropRight(1)
  }
}
