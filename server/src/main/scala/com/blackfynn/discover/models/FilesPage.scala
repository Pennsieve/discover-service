// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import cats.implicits._
import com.blackfynn.discover.models._
import com.blackfynn.discover.{ utils, Config }
import com.blackfynn.discover.db.PublicDatasetVersionsMapper
import com.blackfynn.discover.server.definitions
import com.blackfynn.discover.clients.FileSearchResponse
import io.scalaland.chimney.dsl._

object FilesPage {
  def apply(
    searchResponse: FileSearchResponse
  )(implicit
    config: Config
  ): definitions.FilesPage =
    searchResponse
      .into[definitions.FilesPage]
      .withFieldComputed(
        _.files,
        _ =>
          searchResponse.files
            .map(
              _.into[definitions.FileSearchResponse]
                .withFieldComputed(_.datasetId, _.dataset.id)
                .withFieldComputed(_.datasetVersion, _.dataset.version)
                .withFieldComputed(_.size, _.file.size)
                .withFieldComputed(_.fileType, _.file.fileType.toString)
                .withFieldComputed(_.sourcePackageId, _.file.sourcePackageId)
                .withFieldComputed(
                  _.packageType,
                  r => utils.getPackageType(r.file.fileType)
                )
                .withFieldComputed(_.icon, r => utils.getIcon(r.file.fileType))
                .transform
            )
            .toIndexedSeq
      )
      .transform
}
