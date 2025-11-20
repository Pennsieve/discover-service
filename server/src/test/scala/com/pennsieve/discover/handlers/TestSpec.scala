// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import com.pennsieve.discover.db.PublicDatasetsMapper
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TestSpec extends AnyWordSpec with Matchers with ServiceSpecHarness {

  "ports" should {
    "be non-null" in {
      ports shouldNot be(null)
    }

    "be able to store a test object" in {
      val datasetVersion =
        TestUtilities.createDatasetV1(ports.db)(sourceDatasetId = 17)

      datasetVersion shouldNot be(null)

      val dataset = ports.db
        .run(PublicDatasetsMapper.getDataset(datasetVersion.datasetId))
        .awaitFinite()

      dataset shouldNot be(null)
      dataset.sourceDatasetId shouldBe 17
    }
  }
}
