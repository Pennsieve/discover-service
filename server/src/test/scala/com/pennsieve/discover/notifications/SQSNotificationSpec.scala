// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.discover.models.S3Bucket
import com.pennsieve.discover.notifications.SQSNotificationType.INDEX
import com.pennsieve.models.PublishStatus
import io.circe.parser.decode
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SQSNotificationSpec extends AnyWordSpec with Suite with Matchers {

  "PublishNotification" should {
    "decode strings as integers in a message from SQS" in {

      val json =
        """{
          |  "job_type": "PUBLISH",
          |  "organization_id" : "1",
          |  "dataset_id" : "2",
          |  "status" : "PUBLISH_SUCCEEDED",
          |  "version" : "5"
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        PublishNotification(
          organizationId = 1,
          datasetId = 2,
          status = PublishStatus.PublishSucceeded,
          version = 5,
          error = None
        )
      )
    }

    "decode an optional error message" in {

      val json =
        """{
          |  "job_type": "PUBLISH",
          |  "organization_id" : "1",
          |  "dataset_id" : "2",
          |  "status" : "PUBLISH_FAILED",
          |  "version" : "5",
          |  "error": "Oh no!"
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        PublishNotification(
          organizationId = 1,
          datasetId = 2,
          status = PublishStatus.PublishFailed,
          version = 5,
          error = Some("Oh no!")
        )
      )
    }

    "decode publish job without explicit job_type" in {
      val json =
        """{
          |  "organization_id" : "1",
          |  "dataset_id" : "2",
          |  "status" : "PUBLISH_SUCCEEDED",
          |  "version" : "5"
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        PublishNotification(
          organizationId = 1,
          datasetId = 2,
          status = PublishStatus.PublishSucceeded,
          version = 5,
          error = None
        )
      )
    }

  }

  "ReleaseNotification" should {
    "decode strings as integers in a message from SQS" in {

      val json =
        """{
          |  "job_type": "RELEASE",
          |  "organization_id" : "1",
          |  "dataset_id" : "2",
          |  "version" : "5",
          |  "publish_bucket" : "publish-bucket",
          |  "embargo_bucket" : "embargo-bucket",
          |  "success" : true
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        ReleaseNotification(
          organizationId = 1,
          datasetId = 2,
          version = 5,
          publishBucket = S3Bucket("publish-bucket"),
          embargoBucket = S3Bucket("embargo-bucket"),
          success = true,
          error = None
        )
      )
    }
  }

  "ScanForReleaseNotification" should {
    "decode notification from SQS" in {

      val json =
        """{
          |  "job_type": "SCAN_FOR_RELEASE"
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(ScanForReleaseNotification())
    }
  }

  "IndexDatasetRequest" should {
    "decode notification from SQS" in {
      val json =
        """{
          |  "job_type": "INDEX",
          |  "dataset_id": 1,
          |  "version": 1
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        IndexDatasetRequest(jobType = INDEX, datasetId = 1, version = 1)
      )
    }
  }
}
