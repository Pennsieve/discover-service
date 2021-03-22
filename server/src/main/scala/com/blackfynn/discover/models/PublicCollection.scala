// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

final case class PublicCollection(
  name: String,
  sourceCollectionId: Int,
  datasetId: Int,
  versionId: Int,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0
) {}

object PublicCollection {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
