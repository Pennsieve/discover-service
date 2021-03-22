// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.models.FileManifest
import com.blackfynn.discover.server.definitions
import com.blackfynn.discover.utils.{ getFileType, joinPath }
import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

final case class FileDocument(
  file: FileManifest,
  dataset: definitions.PublicDatasetDTO,
  name: String,
  uri: String
)

object FileDocument {
  implicit val encoder: Encoder[FileDocument] = deriveEncoder[FileDocument]
  implicit val decoder: Decoder[FileDocument] = deriveDecoder[FileDocument]

  def apply(
    fileManifest: FileManifest,
    datasetDocument: DatasetDocument
  ): FileDocument =
    FileDocument(fileManifest, datasetDocument.dataset)

  def apply(
    fileManifest: FileManifest,
    datasetDto: definitions.PublicDatasetDTO
  ): FileDocument =
    FileDocument(
      fileManifest,
      datasetDto,
      fileManifest.name,
      joinPath(datasetDto.uri, fileManifest.path)
    )

  def apply(file: PublicFile, datasetDocument: DatasetDocument): FileDocument =
    FileDocument(
      FileManifest(
        path = file.s3Key
          .removeDatasetPrefix(S3Key.Dataset(file.datasetId)),
        size = file.size,
        fileType = getFileType(file.fileType),
        sourcePackageId = file.sourcePackageId
      ),
      datasetDocument.dataset
    )
}
