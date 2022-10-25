// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.models.S3Bucket
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues._
import software.amazon.awssdk.arns.Arn

class ConfigSpec extends AnyWordSpec with Matchers {

  "load" should {
    "correctly parse the external-publish-buckets section" in {
      val config: Config = Config.load

      config.externalPublishBuckets.isEmpty should be(false)

      config.externalPublishBuckets
        .get(S3Bucket("external-bucket-1"))
        .value should be(
        Arn.fromString(
          "arn:aws:iam::000000000000:role/external-bucket-1-access-role"
        )
      )

      config.externalPublishBuckets
        .get(S3Bucket("default-bucket"))
        .isDefined should be(false)
    }
  }
}
