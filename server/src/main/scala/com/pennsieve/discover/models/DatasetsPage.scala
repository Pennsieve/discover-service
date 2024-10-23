// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import cats.implicits._
import com.pennsieve.discover.Config
import com.pennsieve.discover.clients.DatasetSearchResponse
import com.pennsieve.discover.db.PublicDatasetVersionsMapper
import com.pennsieve.discover.server.definitions
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
                externalPublications,
                release
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
                  None,
                  release
                )
          }.toVector
      )
      .transform

  def apply(searchResult: DatasetSearchResponse): definitions.DatasetsPage =
    searchResult
      .into[definitions.DatasetsPage]
      .withFieldComputed(_.datasets, _.datasets.map(_.dataset).to(Vector))
      .transform
}
