// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.server.definitions.DatasetPublishStatus
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.{ ServiceSpecHarness, TestUtilities }
import com.pennsieve.discover.models.PublicDataset
import com.pennsieve.models.{ License, PublishStatus }
import com.pennsieve.models.PublishStatus.{
  PublishInProgress,
  PublishSucceeded
}
import com.pennsieve.test.AwaitableImplicits
import io.circe.syntax._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PublicDatasetsMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  "PublicDatasetsMapper" should {

    "create a new public dataset and retrieve it" in {
      val newPublicDataset: PublicDataset = TestUtilities.createDataset(
        ports.db
      )(name = "My Published Dataset", tags = List("tag"))

      val result: PublicDataset = ports.db
        .run(
          PublicDatasetsMapper
            .getDataset(newPublicDataset.id)
        )
        .await

      assert(result.id == newPublicDataset.id)
      assert(result.name == "My Published Dataset")
      assert(result.sourceOrganizationId == 1)
      assert(result.sourceDatasetId == 1)
      assert(result.ownerId == 1)
      assert(result.ownerFirstName == "Fynn")
      assert(result.ownerLastName == "Blackwell")
      assert(result.ownerOrcid == "0000-0001-2345-6789")
      assert(result.license == License.`Apache 2.0`)
      assert(result.tags == List("tag"))

    }

    "update values and not create duplicate public datasets" in {
      val publicDataset1 = TestUtilities.createDataset(ports.db)(
        name = "My Published Dataset",
        tags = List("tag")
      )

      val publicDataset2 = TestUtilities.createDataset(ports.db)(
        name = "A different name",
        tags = List("red", "blue"),
        ownerId = 227,
        ownerFirstName = "Some",
        ownerLastName = "Owner"
      )

      publicDataset1.id shouldBe publicDataset2.id

      ports.db
        .run(TableQuery[PublicDatasetsTable].length.result)
        .awaitFinite() shouldBe 1

      val updatedDataset = ports.db
        .run(PublicDatasetsMapper.getDataset(publicDataset1.id))
        .awaitFinite()

      updatedDataset should have(
        Symbol("name")("A different name"),
        Symbol("tags")(List("red", "blue")),
        Symbol("ownerId")(227),
        Symbol("ownerFirstName")("Some"),
        Symbol("ownerLastName")("Owner"),
        Symbol("createdAt")(publicDataset1.createdAt)
      )

      // Postgres trigger updates timestamp
      updatedDataset.updatedAt should be > publicDataset1.updatedAt
    }

    "retrieve all public datasets" in {
      for (i <- 1 until 11) {
        TestUtilities.createDataset(ports.db)(
          name = s"My Published Dataset $i",
          sourceOrganizationId = i,
          sourceDatasetId = i,
          tags = List("tag")
        )
      }

      val result: List[PublicDataset] = ports.db
        .run(PublicDatasetsMapper.getDatasets().result.map(_.toList))
        .await

      assert(result.size == 10)
    }
  }

  "filter datasets by tag" in {
    TestUtilities.createDataset(ports.db)(
      sourceDatasetId = 1,
      tags = List("tag1", "tag2")
    )
    TestUtilities.createDataset(ports.db)(
      sourceDatasetId = 2,
      tags = List("tag1")
    )
    TestUtilities.createDataset(ports.db)(
      sourceDatasetId = 3,
      tags = List("tag3")
    )

    val result: List[PublicDataset] = ports.db
      .run(
        PublicDatasetsMapper
          .getDatasets(Some(List("tag1")))
          .result
          .map(_.toList)
      )
      .await

    assert(result.size == 2)
  }

}
