// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.models.PublishStatus
import io.circe.parser.decode
import org.scalatest.{ Matchers, Suite, WordSpec }

class SQSNotificationSpec extends WordSpec with Suite with Matchers {

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
          |  "success" : true
          |}""".stripMargin

      decode[SQSNotification](json) shouldBe Right(
        ReleaseNotification(
          organizationId = 1,
          datasetId = 2,
          version = 5,
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
}
