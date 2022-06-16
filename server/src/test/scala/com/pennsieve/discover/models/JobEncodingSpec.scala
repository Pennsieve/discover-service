// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.syntax._
import org.scalatest.Suite

import java.time.{ OffsetDateTime, ZoneOffset }
import com.pennsieve.models.{ Degree, RelationshipType }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class JobEncodingSpec extends AnyWordSpec with Suite with Matchers {

  "PublishJob" should {
    "encode integers as strings" in {

      val contrib1 =
        PublicContributor(
          firstName = "Alfred",
          middleInitial = Some("C"),
          lastName = "Kinsey",
          degree = Some(Degree.PhD),
          orcid = None,
          datasetId = 2,
          versionId = 1,
          sourceContributorId = 46,
          sourceUserId = None,
          createdAt = OffsetDateTime.now(ZoneOffset.UTC),
          updatedAt = OffsetDateTime.now(ZoneOffset.UTC),
          id = 1
        )
      val collection1 =
        PublicCollection(
          name = "My Awesome Collection",
          sourceCollectionId = 2,
          datasetId = 1,
          versionId = 1
        )

      val externalPublication = PublicExternalPublication(
        doi = "10.26275/t6j6-77pu",
        relationshipType = RelationshipType.IsSourceOf,
        datasetId = 1,
        version = 1
      )

      val job = PublishJob(
        organizationId = 1,
        organizationNodeId = "N:organization:abc123",
        organizationName = "University of Pennsylvania",
        datasetId = 2,
        datasetNodeId = "N:dataset:abc123",
        publishedDatasetId = 1,
        userId = 3,
        userNodeId = "N:user:abc123",
        userFirstName = "BigBrain",
        userLastName = "Bill",
        userOrcid = "1234-5678-0987-5432",
        s3Bucket = S3Bucket("publish-bucket"),
        s3PgdumpKey = S3Key.Version(2, 5) / "dump.sql",
        s3PublishKey = S3Key.Version(2, 5),
        version = 5,
        doi = "10.324/4529",
        contributors = List(contrib1),
        collections = List(collection1),
        externalPublications = List(externalPublication)
      )

      val json = job.asJson.toString

      json shouldBe
        s"""{
          |  "organization_id" : "1",
          |  "organization_node_id" : "N:organization:abc123",
          |  "organization_name" : "University of Pennsylvania",
          |  "dataset_id" : "2",
          |  "dataset_node_id" : "N:dataset:abc123",
          |  "published_dataset_id" : "1",
          |  "user_id" : "3",
          |  "user_node_id" : "N:user:abc123",
          |  "user_first_name" : "BigBrain",
          |  "user_last_name" : "Bill",
          |  "user_orcid" : "1234-5678-0987-5432",
          |  "s3_bucket" : "publish-bucket",
          |  "s3_pgdump_key" : "2/5/dump.sql",
          |  "s3_publish_key" : "2/5/",
          |  "version" : "5",
          |  "doi" : "10.324/4529",
          |  "contributors" : "[{\\"id\\":1,\\"first_name\\":\\"Alfred\\",\\"middle_initial\\":\\"C\\",\\"last_name\\":\\"Kinsey\\",\\"degree\\":\\"Ph.D.\\",\\"orcid\\":null}]",
          |  "collections" : "[{\\"name\\":\\"My Awesome Collection\\",\\"source_collection_id\\":2,\\"dataset_id\\":1,\\"version_id\\":1}]",
          |  "external_publications" : "[{\\"doi\\":\\"10.26275/t6j6-77pu\\",\\"relationshipType\\":\\"IsSourceOf\\"}]"
          |}""".stripMargin

    }
  }

  "EmbargoReleaseJob" should {

    "encode integers as strings" in {
      val job =
        EmbargoReleaseJob(1, 10, 7, S3Key.Version(2, 5), S3Bucket("bucket"))

      job.asJson.toString shouldBe
        s"""{
          |  "organization_id" : "1",
          |  "dataset_id" : "10",
          |  "version" : "7",
          |  "s3_key" : "2/5/",
          |  "s3_bucket" : "bucket"
          |}""".stripMargin
    }
  }
}
