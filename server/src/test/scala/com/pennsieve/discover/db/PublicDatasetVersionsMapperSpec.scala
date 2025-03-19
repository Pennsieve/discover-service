// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  OrderBy,
  OrderDirection,
  PublicDataset,
  PublicDatasetVersion,
  S3Key
}
import com.pennsieve.discover.server.definitions.{
  DatasetPublishStatus,
  DatasetsPage
}
import com.pennsieve.discover.{
  DatasetUnpublishedException,
  ServiceSpecHarness,
  TestUtilities
}
import com.pennsieve.models.PublishStatus
import com.pennsieve.models.PublishStatus.{
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.pennsieve.test.AwaitableImplicits
import org.postgresql.util.PSQLException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PublicDatasetVersionsMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  "PublicDatasetVersionsMapper" should {

    "create a new public dataset version and retrieve it" in {

      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)()

      assert(publicDatasetV1.version == 1)
      assert(publicDatasetV1.size == 1000L)
      assert(publicDatasetV1.description == "this is a test")
      assert(publicDatasetV1.modelCount == Map.empty[String, Long])
      assert(publicDatasetV1.fileCount == 0L)
      assert(publicDatasetV1.recordCount == 0L)
      assert(publicDatasetV1.s3Bucket.value == "bucket")
      assert(
        publicDatasetV1.s3Key == S3Key.Version(publicDatasetV1.datasetId, 1)
      )
      assert(publicDatasetV1.status == PublishStatus.NotPublished)

      val publicDatasetV2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDatasetV1.datasetId,
        size = 50000L,
        description = "another version",
        modelCount = Map[String, Long]("myModel" -> 50L),
        fileCount = 300L,
        recordCount = 50L,
        status = PublishStatus.PublishInProgress
      )

      assert(publicDatasetV2.version == 2)
      assert(publicDatasetV2.size == 50000L)
      assert(publicDatasetV2.description == "another version")
      assert(publicDatasetV2.modelCount == Map[String, Long]("myModel" -> 50L))
      assert(publicDatasetV2.fileCount == 300L)
      assert(publicDatasetV2.recordCount == 50L)
      assert(publicDatasetV2.s3Bucket.value == s"bucket")
      assert(
        publicDatasetV2.s3Key == S3Key.Version(publicDatasetV1.datasetId, 2)
      )
      assert(publicDatasetV2.status == PublishStatus.PublishInProgress)
    }

    // Note: This test is no longer valid with the Publishing 5.0 capability
    // (all dataset versions for the same published dataset have the same s3_key)
//    "version s3 key must be unique" in {
//
//      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)()
//      val publicDatasetV2 = TestUtilities.createNewDatasetVersion(ports.db)(
//        id = publicDatasetV1.datasetId
//      )
//
//      intercept[PSQLException] {
//        ports.db
//          .run(sql"""
//               UPDATE public_dataset_versions
//               SET s3_key = ${publicDatasetV2.s3Key.value}
//               WHERE version = ${publicDatasetV1.version}
//               """.as[Int])
//          .awaitFinite()
//      }.getMessage() should include(
//        "duplicate key value violates unique constraint"
//      )
//    }

    "s3 key must be formatted :datasetId/:version/" in {

      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)()

      for {
        badS3Key <- List("", "1/", "1/2", "/2/", "/1/2/", "a/b/")
      } yield
        intercept[PSQLException] {
          ports.db
            .run(sql"""
                 UPDATE public_dataset_versions
                 SET s3_key = $badS3Key
                 WHERE version = ${publicDatasetV1.version}
                 """.as[Int])
            .awaitFinite()
        }.getMessage() should include("violates check constraint")
    }

    "update updatedAt Postgres trigger" in {
      val v1 = TestUtilities.createDatasetV1(ports.db)()

      ports.db
        .run(
          PublicDatasetVersionsMapper
            .setStatus(v1.datasetId, v1.version, PublishStatus.Unpublished)
        )
        .awaitFinite()

      val updated = ports.db
        .run(PublicDatasetVersionsMapper.getVersion(v1.datasetId, v1.version))
        .awaitFinite()
      updated.updatedAt should be > v1.updatedAt
    }

    "retrieve a public dataset version" in {
      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)()

      val result = ports.db
        .run(
          PublicDatasetVersionsMapper.getVersion(publicDatasetV1.datasetId, 1)
        )
        .await

      assert(result.datasetId == publicDatasetV1.datasetId)
      assert(result.version == 1)

    }

    "retrieve the latest public dataset version" in {
      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)()
      val publicDatasetV2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDatasetV1.datasetId
      )

      val result = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDatasetV1.datasetId)
        )
        .await
        .get

      assert(result.datasetId == publicDatasetV1.datasetId)
      assert(result.version == 2)

    }

    "retrieve a published public dataset version" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishStatus.PublishSucceeded
        )

      val result = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVisibleVersion(dataset, 1)
        )
        .await

      assert(result.datasetId == version.datasetId)
      assert(result.version == 1)
    }

    "retrieve an embargoed dataset version" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishStatus.EmbargoSucceeded
        )

      val result = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVisibleVersion(dataset, 1)
        )
        .await

      assert(result.datasetId == version.datasetId)
      assert(result.version == 1)
    }

    "return an exception when the version is unpublished" in {
      val dataset = TestUtilities.createDataset(ports.db)()
      val version = TestUtilities.createNewDatasetVersion(ports.db)(
        id = dataset.id,
        status = PublishStatus.Unpublished
      )

      val result = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getVisibleVersion(dataset, 1)
        )

      whenReady(result.failed) {
        _ shouldBe DatasetUnpublishedException(dataset, version)
      }
    }

    "retrieve a paginated list of the latest published dataset versions" in {
      val ds1 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 1, name = "A")
      val ds2 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 2, name = "B")
      val ds3 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 3, name = "C")
      val ds4 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 4, name = "D")
      val ds1_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded
      )
      val ds2_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds2.id,
        status = PublishSucceeded,
        size = 100L
      )
      val ds3_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds3.id,
        status = PublishSucceeded,
        size = 90L
      )
      val ds4_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds4.id,
        status = PublishSucceeded,
        size = 80L
      )
      val ds1_v2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded,
        size = 70L
      )

      TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = ds1.id,
        organizationId = 1,
        version = ds1_v1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      TestUtilities.createContributor(ports.db)(
        firstName = "Tom",
        lastName = "Hanks",
        orcid = None,
        datasetId = ds2.id,
        organizationId = 1,
        version = ds2_v1.version,
        sourceContributorId = 2,
        sourceUserId = Some(2)
      )
      TestUtilities.createContributor(ports.db)(
        firstName = "Tony",
        lastName = "Parker",
        orcid = None,
        datasetId = ds3.id,
        organizationId = 1,
        version = ds3_v1.version,
        sourceContributorId = 3,
        sourceUserId = Some(3)
      )
      TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = ds4.id,
        organizationId = 1,
        version = ds4_v1.version,
        sourceContributorId = 4,
        sourceUserId = Some(4)
      )
      TestUtilities.createContributor(ports.db)(
        firstName = "Henry",
        lastName = "Winkler",
        orcid = None,
        datasetId = ds1.id,
        organizationId = 1,
        version = ds1_v2.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )
      val page1 = ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 2,
            offset = 0,
            orderBy = OrderBy.Relevance,
            orderDirection = OrderDirection.Descending
          )
        )
        .await

      assert(page1.datasets.size == 2)
      assert(page1.datasets.map(_._1.id) == List(ds1.id, ds4.id))

      val page2 = ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 2,
            offset = 2,
            orderBy = OrderBy.Relevance,
            orderDirection = OrderDirection.Descending
          )
        )
        .await

      assert(page2.datasets.size == 2)
      assert(page2.datasets.map(_._1.id) == List(ds3.id, ds2.id))

      // order by name

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Name,
            orderDirection = OrderDirection.Ascending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds1.id, ds2.id, ds3.id, ds4.id)

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Name,
            orderDirection = OrderDirection.Descending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds4.id, ds3.id, ds2.id, ds1.id)

      // order by date created

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Date,
            orderDirection = OrderDirection.Ascending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds2.id, ds3.id, ds4.id, ds1.id)

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Date,
            orderDirection = OrderDirection.Descending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds1.id, ds4.id, ds3.id, ds2.id)

      // order by size

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Size,
            orderDirection = OrderDirection.Ascending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds1.id, ds4.id, ds3.id, ds2.id)

      ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = None,
            limit = 10,
            offset = 0,
            orderBy = OrderBy.Size,
            orderDirection = OrderDirection.Descending
          )
        )
        .await
        .datasets
        .map(_._1.id) shouldBe List(ds2.id, ds3.id, ds4.id, ds1.id)

    }

    "retrieve a list of the latest published dataset versions based on a list of ids" in {
      val ds1 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 1, name = "A")
      val ds2 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 2, name = "B")
      val ds3 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 3, name = "C")
      val ds4 =
        TestUtilities.createDataset(ports.db)(sourceDatasetId = 4, name = "D")
      val ds1_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded
      )
      val ds2_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds2.id,
        status = PublishSucceeded
      )
      val ds3_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds3.id,
        status = PublishSucceeded
      )
      val ds4_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds4.id,
        status = PublishSucceeded
      )
      val ds1_v2 = TestUtilities.createNewDatasetVersion(ports.db)(
        id = ds1.id,
        status = PublishSucceeded
      )

      val page = ports.db
        .run(
          PublicDatasetVersionsMapper
            .getPagedDatasets(
              ids = Some(List(ds1.id, ds2.id)),
              limit = 2,
              offset = 0,
              orderBy = OrderBy.Date,
              orderDirection = OrderDirection.Ascending,
              tags = None
            )
        )
        .await

      assert(page.datasets.size == 2)
      assert(page.datasets.map(_._1.id) == List(ds2.id, ds1.id))
      assert(page.datasets.map(_._2.version) == List(1, 2))

    }

    "change the status of a public dataset version" in {
      val publicDatasetV1 = TestUtilities.createDatasetV1(ports.db)(
        status = PublishStatus.NotPublished
      )

      ports.db
        .run(
          PublicDatasetVersionsMapper.setStatus(
            publicDatasetV1.datasetId,
            publicDatasetV1.version,
            PublishStatus.PublishSucceeded
          )
        )
        .await

      ports.db
        .run(
          PublicDatasetVersionsMapper
            .getLatestVersion(publicDatasetV1.datasetId)
        )
        .await
        .map(_.status) shouldBe Some(PublishStatus.PublishSucceeded)
    }
  }

  "return the status of a published dataset" in {
    val publicDataset = TestUtilities.createDataset(ports.db)()
    TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishSucceeded
    )
    val publicDatasetV2 =
      TestUtilities.createNewDatasetVersion(ports.db)(
        id = publicDataset.id,
        status = PublishSucceeded
      )

    val expected = DatasetPublishStatus(
      publicDataset.name,
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      Some(publicDataset.id),
      2,
      PublishSucceeded,
      Some(publicDatasetV2.createdAt)
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId
          )
      )
      .await

    assert(result == expected)
  }

  "return the status of a dataset with publish in progress" in {
    val publicDataset = TestUtilities.createDataset(ports.db)()
    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishSucceeded
    )
    TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishStatus.PublishInProgress
    )

    val expected = DatasetPublishStatus(
      publicDataset.name,
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      Some(publicDataset.id),
      1,
      PublishStatus.PublishInProgress,
      Some(publicDatasetV1.createdAt)
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId
          )
      )
      .await

    assert(result == expected)
  }

  "return the status of an unpublished dataset" in {
    val publicDataset = TestUtilities.createDataset(ports.db)()
    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = Unpublished
    )

    val expected = DatasetPublishStatus(
      publicDataset.name,
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      Some(publicDataset.id),
      0,
      PublishStatus.Unpublished,
      Some(publicDatasetV1.createdAt)
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId
          )
      )
      .await

    assert(result == expected)
  }

  "return the status of a dataset that has never been published" in {
    val expected = DatasetPublishStatus(
      "",
      12345,
      12345,
      None,
      0,
      PublishStatus.NotPublished,
      None
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(12345, 12345)
      )
      .await

    assert(result == expected)
  }

  "delete the latest version of a dataset in a PublishFailed state" in {
    val publicDataset = TestUtilities.createDataset(ports.db)()
    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishSucceeded
    )
    TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishFailed
    )

    ports.db
      .run(PublicDatasetVersionsMapper.rollbackIfNeeded(publicDataset))
      .await

    val expected = DatasetPublishStatus(
      publicDataset.name,
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      Some(publicDataset.id),
      1,
      PublishStatus.PublishSucceeded,
      Some(publicDatasetV1.createdAt)
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId
          )
      )
      .await

    assert(result == expected)
  }

  "do nothing if the latest version of a dataset is not in a failed state" in {

    val publicDataset = TestUtilities.createDataset(ports.db)()
    val publicDatasetV1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = publicDataset.id,
      status = PublishSucceeded
    )

    ports.db
      .run(PublicDatasetVersionsMapper.rollbackIfNeeded(publicDataset))
      .await

    val expected = DatasetPublishStatus(
      publicDataset.name,
      publicDataset.sourceOrganizationId,
      publicDataset.sourceDatasetId,
      Some(publicDataset.id),
      1,
      PublishStatus.PublishSucceeded,
      Some(publicDatasetV1.createdAt)
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper
          .getDatasetStatus(
            publicDataset.sourceOrganizationId,
            publicDataset.sourceDatasetId
          )
      )
      .await

    assert(result == expected)
  }

  "compute whether a dataset is under embargo" in {
    val version = TestUtilities.createDatasetV1(ports.db)()

    import PublishStatus._

    for {
      status <- List(
        NotPublished,
        PublishInProgress,
        PublishFailed,
        PublishSucceeded,
        Unpublished
      )
    } yield version.copy(status = status).underEmbargo shouldBe false

    for {
      status <- List(
        EmbargoInProgress,
        EmbargoFailed,
        EmbargoSucceeded,
        ReleaseInProgress,
        ReleaseFailed
      )
    } yield version.copy(status = status).underEmbargo shouldBe true
  }
}
