// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover
import com.pennsieve.discover.models.{
  PublicDataset,
  PublicDatasetVersion,
  S3Bucket,
  S3Key
}
import com.pennsieve.models.PublishStatus
import org.scalatest.matchers.{ HavePropertyMatchResult, HavePropertyMatcher }

import java.time.LocalDate
import scala.reflect.ClassTag

object CustomMatchers {

  def makeHavePropertyMatcher[T: ClassTag, P: ClassTag](
    propertyName: String,
    expectedValue: P,
    actualValueAccessor: T => P
  ): HavePropertyMatcher[T, P] = {
    val equalTo: P => Boolean = _ == expectedValue
    val matchTest: T => Boolean = actualValueAccessor andThen equalTo
    HavePropertyMatcher { (objectWithProp: T) =>
      HavePropertyMatchResult(
        matchTest(objectWithProp),
        propertyName,
        expectedValue,
        actualValueAccessor(objectWithProp)
      )

    }
  }

  trait PublicDatasetMatchers {

    def name(expectedName: String): HavePropertyMatcher[PublicDataset, String] =
      makeHavePropertyMatcher("name", expectedName, _.name)

    def sourceOrganizationId(
      expectedId: Int
    ): HavePropertyMatcher[PublicDataset, Int] = makeHavePropertyMatcher(
      "sourceOrganizationId",
      expectedId,
      _.sourceOrganizationId
    )

    def sourceDatasetId(
      expectedId: Int
    ): HavePropertyMatcher[PublicDataset, Int] =
      makeHavePropertyMatcher("sourceDatasetId", expectedId, _.sourceDatasetId)

    def ownerId(expectedId: Int): HavePropertyMatcher[PublicDataset, Int] =
      makeHavePropertyMatcher("ownerId", expectedId, _.ownerId)

    def ownerFirstName(
      expectedFirstName: String
    ): HavePropertyMatcher[PublicDataset, String] =
      makeHavePropertyMatcher(
        "ownerFirstName",
        expectedFirstName,
        _.ownerFirstName
      )

    def ownerLastName(
      expectedLastName: String
    ): HavePropertyMatcher[PublicDataset, String] =
      makeHavePropertyMatcher(
        "ownerLastName",
        expectedLastName,
        _.ownerLastName
      )

    def ownerOrcid(
      expectedOrcid: String
    ): HavePropertyMatcher[PublicDataset, String] =
      makeHavePropertyMatcher("ownerOrcid", expectedOrcid, _.ownerOrcid)

  }

  trait PublicDatasetVersionMatchers {
    def version(expected: Int): HavePropertyMatcher[PublicDatasetVersion, Int] =
      makeHavePropertyMatcher("version", expected, _.version)

    def modelCount(
      expected: Map[String, Long]
    ): HavePropertyMatcher[PublicDatasetVersion, Map[String, Long]] =
      makeHavePropertyMatcher("modelCount", expected, _.modelCount)

    def recordCount(
      expected: Long
    ): HavePropertyMatcher[PublicDatasetVersion, Long] =
      makeHavePropertyMatcher("recordCount", expected, _.recordCount)

    def fileCount(
      expected: Long
    ): HavePropertyMatcher[PublicDatasetVersion, Long] =
      makeHavePropertyMatcher("fileCount", expected, _.fileCount)

    def size(expected: Long): HavePropertyMatcher[PublicDatasetVersion, Long] =
      makeHavePropertyMatcher("size", expected, _.size)

    def description(
      expected: String
    ): HavePropertyMatcher[PublicDatasetVersion, String] =
      makeHavePropertyMatcher("description", expected, _.description)

    def status(
      expected: PublishStatus
    ): HavePropertyMatcher[PublicDatasetVersion, PublishStatus] =
      makeHavePropertyMatcher("status", expected, _.status)

    def s3Bucket(
      expected: S3Bucket
    ): HavePropertyMatcher[PublicDatasetVersion, S3Bucket] =
      makeHavePropertyMatcher("s3Bucket", expected, _.s3Bucket)

    def s3Key(
      expected: S3Key.Version
    ): HavePropertyMatcher[PublicDatasetVersion, S3Key.Version] =
      makeHavePropertyMatcher("s3Key", expected, _.s3Key)

    def doi(
      expected: String
    ): HavePropertyMatcher[PublicDatasetVersion, String] =
      makeHavePropertyMatcher("doi", expected, _.doi)

    def embargoReleaseDate(
      expected: Option[LocalDate]
    ): HavePropertyMatcher[PublicDatasetVersion, Option[LocalDate]] =
      makeHavePropertyMatcher(
        "embargoReleaseDate",
        expected,
        _.embargoReleaseDate
      )
  }
}
