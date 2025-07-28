// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PublicDatasetDoiCollectionDoisMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  "PublicDatasetDoiCollectionDoisMapper" should {

    case class PageParams(limit: Int, offset: Int)

    "get a page of DOIs" in {
      val dataset1 = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(name = "dataset 1", sourceDatasetId = 1)
      val dataset1V =
        TestUtilities.createNewDatasetVersion(ports.db)(dataset1.id)

      TestUtilities.createDatasetDoiCollection(ports.db)(
        datasetId = dataset1V.datasetId,
        datasetVersion = dataset1V.version,
        banners = TestUtilities.randomBannerUrls
      )

      val dataset1VDois = List(
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString()
      )

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.addDOIs(
            datasetId = dataset1V.datasetId,
            datasetVersion = dataset1V.version,
            dois = dataset1VDois
          )
        )
        .awaitFinite()

      val dataset2 = TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(name = "dataset 2", sourceDatasetId = 2)
      val dataset2V =
        TestUtilities.createNewDatasetVersion(ports.db)(dataset2.id)

      TestUtilities.createDatasetDoiCollection(ports.db)(
        datasetId = dataset2V.datasetId,
        datasetVersion = dataset2V.version,
        banners = TestUtilities.randomBannerUrls
      )

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.addDOIs(
            datasetId = dataset2V.datasetId,
            datasetVersion = dataset2V.version,
            dois = List(TestUtilities.randomString())
          )
        )
        .awaitFinite()

      val firstPageParams = PageParams(limit = 3, offset = 0)
      val firstPage = ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.getDOIPage(
            datasetId = dataset1.id,
            datasetVersion = dataset1V.version,
            limit = firstPageParams.limit,
            offset = firstPageParams.offset
          )
        )
        .awaitFinite()

      firstPage shouldBe PagedDoiResult(
        limit = firstPageParams.limit,
        offset = firstPageParams.offset,
        dataset1VDois.size,
        dataset1VDois.slice(
          firstPageParams.offset,
          firstPageParams.offset + firstPageParams.limit
        )
      )

    }
  }
}
