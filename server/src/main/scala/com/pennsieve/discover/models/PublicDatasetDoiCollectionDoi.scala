// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublicDatasetDoiCollectionDoi(
  id: Int = 0,
  datasetId: Int,
  datasetVersion: Int,
  doi: String,
  position: Int,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicDatasetDoiCollectionDoi {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled: (
    (Int, Int, Int, String, Int, OffsetDateTime, OffsetDateTime)
  ) => PublicDatasetDoiCollectionDoi = (this.apply _).tupled
}
