// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import cats.implicits._
import com.blackfynn.discover.Config
import com.blackfynn.discover.server.definitions
import com.blackfynn.discover.clients.RecordSearchResponse
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
