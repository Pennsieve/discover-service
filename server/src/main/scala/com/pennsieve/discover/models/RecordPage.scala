// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import cats.implicits._
import com.pennsieve.discover.Config
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.clients.RecordSearchResponse
import io.scalaland.chimney.dsl._

object RecordPage {

  def apply(
    searchResponse: RecordSearchResponse
  )(implicit
    config: Config
  ): definitions.RecordPage =
    searchResponse
      .into[definitions.RecordPage]
      .withFieldComputed(
        _.records,
        _ =>
          searchResponse.records
            .map(_.record.into[definitions.Record].transform)
            .toIndexedSeq
      )
      .transform
}
