// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.models.S3Bucket
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues._

class ConfigSpec extends AnyWordSpec with Matchers {

  "load" should {
    "correctly parse the external-publish-buckets section" in {
      val config: Config = Config.load

      config.externalPublishBuckets.isEmpty should be(false)

      config.externalPublishBuckets
        .get(S3Bucket("external-bucket-1"))
        .value should be("external-role-1")

      config.externalPublishBuckets
        .get(S3Bucket("default-bucket"))
        .isDefined should be(false)
    }
  }
}
