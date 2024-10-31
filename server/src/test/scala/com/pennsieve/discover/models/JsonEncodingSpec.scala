// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.parser.decode

class JsonEncodingSpec extends AnyWordSpec with Suite with Matchers {
  "JSON Encoding" should {
    "decode a ReleaseAssetListing" in {
      val jsonString =
        s"""
           |{"files":[{"file":"manifest.json","name":"manifest.json","type":"file","size":1234},{"file":"metadata.json","name":"metadata.json","type":"file","size":2345}]}
           |""".stripMargin

      val expected = ReleaseAssetListing(
        files = Seq(
          ReleaseAssetFile(
            file = "manifest.json",
            name = "manifest.json",
            `type` = ReleaseAssetFileType.File,
            size = 1234
          ),
          ReleaseAssetFile(
            file = "metadata.json",
            name = "metadata.json",
            `type` = ReleaseAssetFileType.File,
            size = 2345
          )
        )
      )
      val decoded = decode[ReleaseAssetListing](jsonString)
        .fold(l => (), r => r)

      decoded shouldEqual expected
    }
  }
}
