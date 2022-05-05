// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

import com.pennsieve.models.{ Degree, PublishStatus }

final case class PublicContributor(
  firstName: String,
  middleInitial: Option[String],
  lastName: String,
  degree: Option[Degree],
  orcid: Option[String],
  datasetId: Int,
  versionId: Int,
  sourceContributorId: Int,
  sourceUserId: Option[Int],
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  id: Int = 0
) {
  def givenName: String = {
    s"$firstName ${middleInitial.getOrElse("")}".trim
  }
  def fullName: String = s"$givenName $lastName".trim

}

object PublicContributor {
  /*
   * This is required by slick when using a companion object on a case
   * class that defines a database table
   */
  val tupled = (this.apply _).tupled
}
