// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class PublicDatasetDoiCollection(
  id: Int = 0,
  datasetId: Int,
  datasetVersion: Int,
  banners: List[String] = List.empty,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object PublicDatasetDoiCollection {

  val collectionOrgId = -20
  val collectionOrgName = "Fake Collection Organization"
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled: (
    (Int, Int, Int, List[String], OffsetDateTime, OffsetDateTime)
  ) => PublicDatasetDoiCollection = (this.apply _).tupled
}
