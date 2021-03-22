// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import cats.implicits._
import com.blackfynn.discover.Config
import com.blackfynn.discover.clients.DatasetSearchResponse
import com.blackfynn.discover.db.PublicDatasetVersionsMapper
import com.blackfynn.discover.server.definitions
import io.scalaland.chimney.dsl._

object DatasetsPage {

  def apply(
    pagedResult: PublicDatasetVersionsMapper.PagedDatasetsResult
  )(implicit
    config: Config
  ): definitions.DatasetsPage =
    pagedResult
      .into[definitions.DatasetsPage]
      .withFieldComputed(
        _.datasets,
        _ =>
          pagedResult.datasets.map {
            case (
                dataset,
                version,
                contributors,
                sponsorship,
                revision,
                collections,
                externalPublications
                ) =>
              PublicDatasetDTO
                .apply(
                  dataset,
                  version,
                  contributors,
                  sponsorship,
                  revision,
                  collections,
                  externalPublications,
                  None
                )
          }.toIndexedSeq
      )
      .transform

  def apply(searchResult: DatasetSearchResponse): definitions.DatasetsPage =
    searchResult
      .into[definitions.DatasetsPage]
      .withFieldComputed(_.datasets, _.datasets.map(_.dataset).to[IndexedSeq])
      .transform
}
