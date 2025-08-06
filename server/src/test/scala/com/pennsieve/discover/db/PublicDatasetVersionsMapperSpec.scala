// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.{
  OrderBy,
  OrderDirection,
  PublicDataset,
  PublicDatasetDoiCollectionWithSize,
  PublicDatasetVersion,
  PublishingWorkflow,
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
  EmbargoSucceeded,
  PublishFailed,
  PublishInProgress,
  PublishSucceeded,
  Unpublished
}
import com.pennsieve.models.RelationshipType.{ Documents, References, Requires }
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
      Some(publicDatasetV2.createdAt),
      workflowId =
        PublishingWorkflowIdentifier.workflowid(Some(publicDatasetV2))
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
      Some(publicDatasetV1.createdAt),
      workflowId =
        PublishingWorkflowIdentifier.workflowid(Some(publicDatasetV1))
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
      Some(publicDatasetV1.createdAt),
      workflowId =
        PublishingWorkflowIdentifier.workflowid(Some(publicDatasetV1))
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
      None,
      workflowId = PublishingWorkflow.Unknown
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
      Some(publicDatasetV1.createdAt),
      workflowId =
        PublishingWorkflowIdentifier.workflowid(Some(publicDatasetV1))
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
      Some(publicDatasetV1.createdAt),
      workflowId =
        PublishingWorkflowIdentifier.workflowid(Some(publicDatasetV1))
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

  "retrieve dataset versions for given DOIs" in {
    val ds1 =
      TestUtilities.createDataset(ports.db)(sourceDatasetId = 1, name = "A")
    val ds2 =
      TestUtilities.createDataset(ports.db)(sourceDatasetId = 2, name = "B")
    val ds3 =
      TestUtilities.createDataset(ports.db)(sourceDatasetId = 3, name = "C")
    val ds4 =
      TestUtilities.createDataset(ports.db)(sourceDatasetId = 4, name = "D")

    val ds5 =
      TestUtilities.createDoiCollectionDataset(
        ports.db,
        ports.config.doiCollections.idSpace
      )(sourceDatasetId = 5, name = "E")

    val ds1_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds1.id,
      status = PublishSucceeded
    )
    val ds2_v1_failed = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds2.id,
      status = PublishFailed,
      size = 100L
    )
    val ds3_v1_embargoed = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds3.id,
      status = EmbargoSucceeded,
      size = 90L
    )
    val ds4_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds4.id,
      status = PublishSucceeded,
      size = 80L
    )
    val ds5_v1 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds5.id,
      status = PublishSucceeded,
      size = 10L
    )
    val ds1_v2 = TestUtilities.createNewDatasetVersion(ports.db)(
      id = ds1.id,
      status = PublishSucceeded,
      size = 70L
    )

    val ds1_v1_contrib = TestUtilities.createContributor(ports.db)(
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
      version = ds2_v1_failed.version,
      sourceContributorId = 2,
      sourceUserId = Some(2)
    )
    val ds3_v1_embargoed_contrib = TestUtilities.createContributor(ports.db)(
      firstName = "Tony",
      lastName = "Parker",
      orcid = None,
      datasetId = ds3.id,
      organizationId = 1,
      version = ds3_v1_embargoed.version,
      sourceContributorId = 3,
      sourceUserId = Some(3)
    )
    val ds4_v1_contrib = TestUtilities.createContributor(ports.db)(
      firstName = "Sally",
      lastName = "Fields",
      orcid = None,
      datasetId = ds4.id,
      organizationId = 1,
      version = ds4_v1.version,
      sourceContributorId = 4,
      sourceUserId = Some(4)
    )
    val ds1_v2_contrib = TestUtilities.createContributor(ports.db)(
      firstName = "Henry",
      lastName = "Winkler",
      orcid = None,
      datasetId = ds1.id,
      organizationId = 1,
      version = ds1_v2.version,
      sourceContributorId = 1,
      sourceUserId = Some(1)
    )

    val ds1_sponsorship = TestUtilities.createSponsorship(ports.db)(
      sourceOrganizationId = ds1.sourceOrganizationId,
      sourceDatasetId = ds1.sourceDatasetId
    )

    val ds4_v1_revision = TestUtilities.createRevision(ports.db)(ds4_v1)

    val ds1_v2_collection = TestUtilities.createCollection(ports.db)(
      datasetId = ds1_v2.datasetId,
      version = ds1_v2.version,
      sourceCollectionId = 1
    )

    val ds4_v1_release = TestUtilities.createDatasetRelease(ports.db)(
      ds4_v1.datasetId,
      ds4_v1.version,
      "GitHub",
      "v1.0.0",
      "https://github.com/Pennsieve/test-repo"
    )

    val ds1_v1_externalPub = TestUtilities.createExternalPublication(ports.db)(
      ds1_v1.datasetId,
      ds1_v1.version,
      References
    )

    val ds4_v1_externalPub1 = TestUtilities.createExternalPublication(ports.db)(
      ds4_v1.datasetId,
      ds4_v1.version,
      Requires
    )

    val ds4_v1_externalPub2 = TestUtilities.createExternalPublication(ports.db)(
      ds4_v1.datasetId,
      ds4_v1.version,
      Documents
    )

    val ds5_v1_doiCollection =
      TestUtilities.createDatasetDoiCollection(ports.db)(
        datasetId = ds5.id,
        datasetVersion = ds5_v1.version,
        banners = TestUtilities.randomBannerUrls
      )

    val ds5_v1_dois =
      List(
        TestUtilities.randomString(),
        TestUtilities.randomString(),
        TestUtilities.randomString()
      )

    ports.db.run(
      PublicDatasetDoiCollectionDoisMapper
        .addDOIs(ds5.id, ds5_v1.version, ds5_v1_dois)
    )

    val ds5_v1_doiCollectionWithSize = PublicDatasetDoiCollectionWithSize(
      id = ds5_v1_doiCollection.id,
      datasetId = ds5_v1_doiCollection.datasetId,
      datasetVersion = ds5_v1_doiCollection.datasetVersion,
      banners = ds5_v1_doiCollection.banners,
      size = ds5_v1_dois.size,
      createdAt = ds5_v1_doiCollection.createdAt,
      updatedAt = ds5_v1_doiCollection.updatedAt
    )

    val result = ports.db
      .run(
        PublicDatasetVersionsMapper.getDatasetsByDoi(
          dois = List(
            ds1_v1.doi,
            ds1_v2.doi,
            ds2_v1_failed.doi,
            ds3_v1_embargoed.doi,
            ds4_v1.doi,
            ds5_v1.doi
          )
        )
      )
      .await

    // Check published
    result.published.size shouldBe 5

    result.published should contain key ds1_v1.doi
    result.published(ds1_v1.doi) shouldBe PublicDatasetVersionsMapper
      .DatasetDetails(
        dataset = ds1,
        version = ds1_v1,
        contributors = IndexedSeq(ds1_v1_contrib),
        sponsorship = Some(ds1_sponsorship),
        revision = None,
        collections = IndexedSeq.empty,
        externalPublications = IndexedSeq(ds1_v1_externalPub),
        release = None,
        doiCollection = None
      )

    result.published should contain key ds1_v2.doi
    result.published(ds1_v2.doi) shouldBe PublicDatasetVersionsMapper
      .DatasetDetails(
        dataset = ds1,
        version = ds1_v2,
        contributors = IndexedSeq(ds1_v2_contrib),
        sponsorship = Some(ds1_sponsorship),
        revision = None,
        collections = IndexedSeq(ds1_v2_collection),
        externalPublications = IndexedSeq.empty,
        release = None,
        doiCollection = None
      )

    result.published should contain key ds3_v1_embargoed.doi
    result.published(ds3_v1_embargoed.doi) shouldBe PublicDatasetVersionsMapper
      .DatasetDetails(
        dataset = ds3,
        version = ds3_v1_embargoed,
        contributors = IndexedSeq(ds3_v1_embargoed_contrib),
        sponsorship = None,
        revision = None,
        collections = IndexedSeq.empty,
        externalPublications = IndexedSeq.empty,
        release = None,
        doiCollection = None
      )

    // Breaking this one up because the external pubs are not returned in a deterministic order
    result.published should contain key ds4_v1.doi
    result.published(ds4_v1.doi).dataset shouldBe ds4
    result.published(ds4_v1.doi).version shouldBe ds4_v1
    result.published(ds4_v1.doi).contributors shouldBe IndexedSeq(
      ds4_v1_contrib
    )
    result.published(ds4_v1.doi).sponsorship shouldBe empty
    result.published(ds4_v1.doi).revision shouldBe Some(ds4_v1_revision)
    result.published(ds4_v1.doi).collections shouldBe empty
    result
      .published(ds4_v1.doi)
      .externalPublications should contain theSameElementsAs IndexedSeq(
      ds4_v1_externalPub1,
      ds4_v1_externalPub2
    )
    result.published(ds4_v1.doi).release shouldBe Some(ds4_v1_release)

    result.published should contain key ds5_v1.doi
    result.published(ds5_v1.doi) shouldBe PublicDatasetVersionsMapper
      .DatasetDetails(
        dataset = ds5,
        version = ds5_v1,
        contributors = IndexedSeq.empty,
        sponsorship = None,
        revision = None,
        collections = IndexedSeq.empty,
        externalPublications = IndexedSeq.empty,
        release = None,
        doiCollection = Some(ds5_v1_doiCollectionWithSize)
      )

    // Check Unpublished
    result.unpublished.size shouldBe 1

    result.unpublished should contain key ds2_v1_failed.doi
    result.unpublished(ds2_v1_failed.doi) shouldBe (ds2, ds2_v1_failed)

  }

  "return empty response when given an empty list of DOIs" in {
    val result = ports.db
      .run(PublicDatasetVersionsMapper.getDatasetsByDoi(dois = List.empty))
      .await

    result.published shouldBe empty
    result.unpublished shouldBe empty

  }
}
