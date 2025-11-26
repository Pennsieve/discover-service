// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import akka.actor.ActorSystem
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PublicDatasetDoiCollectionDoisMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  override implicit val system: ActorSystem = ActorSystem("discover-service")

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  "PublicDatasetDoiCollectionDoisMapper" should {

    case class PageParams(limit: Int, offset: Int)

    "support paginating through all of the DOIs in a DoiCollection" in {
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

      val limit = 3
      for (offset <- 0 until 7 by limit) {
        val pageParams = PageParams(limit = limit, offset = offset)
        val page = ports.db
          .run(
            PublicDatasetDoiCollectionDoisMapper.getDOIPage(
              datasetId = dataset1.id,
              datasetVersion = dataset1V.version,
              limit = pageParams.limit,
              offset = pageParams.offset
            )
          )
          .awaitFinite()

        page shouldBe PagedDoiResult(
          limit = pageParams.limit,
          offset = pageParams.offset,
          dataset1VDois.size,
          dataset1VDois
            .slice(pageParams.offset, pageParams.offset + pageParams.limit)
        )

      }

    }

    "give correct totalCount when limit is zero" in {
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

      val page = ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.getDOIPage(
            datasetId = dataset1.id,
            datasetVersion = dataset1V.version,
            limit = 0,
            offset = 0
          )
        )
        .awaitFinite()

      page shouldBe PagedDoiResult(
        limit = 0,
        offset = 0,
        dataset1VDois.size,
        List.empty
      )

    }

    "give correct totalCount when offset >= number of DOIs" in {
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

      val limit = 10
      val offset = dataset1VDois.size

      val page = ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.getDOIPage(
            datasetId = dataset1.id,
            datasetVersion = dataset1V.version,
            limit = limit,
            offset = offset
          )
        )
        .awaitFinite()

      page shouldBe PagedDoiResult(
        limit = limit,
        offset = offset,
        dataset1VDois.size,
        List.empty
      )

    }

    "return the correct number of DOIs" in {
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

      val dataset2VDois = List(TestUtilities.randomString())

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.addDOIs(
            datasetId = dataset2V.datasetId,
            datasetVersion = dataset2V.version,
            dois = dataset2VDois
          )
        )
        .awaitFinite()

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.getDOICount(
            datasetId = dataset1V.datasetId,
            datasetVersion = dataset1V.version
          )
        )
        .awaitFinite() shouldBe dataset1VDois.size

      ports.db
        .run(
          PublicDatasetDoiCollectionDoisMapper.getDOICount(
            datasetId = dataset2V.datasetId,
            datasetVersion = dataset2V.version
          )
        )
        .awaitFinite() shouldBe dataset2VDois.size

    }

  }

}
