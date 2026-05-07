// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.discover.clients.MockSqsAsyncClient
import io.circe.syntax.EncoderOps
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.LoneElement._

import scala.concurrent.ExecutionContext.Implicits.global

class PublishStorageSyncSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures {

  private val message = PublishStorageSyncMessage(
    1,
    11,
    111,
    "publish-bucket",
    "manifest.json",
    "111/"
  )

  private val expectedEncodedMessage =
    """{"organization_id":1,"dataset_id":11,"public_dataset_id":111,"published_bucket":"publish-bucket","manifest_key":"manifest.json","published_s3_prefix":"111/"}"""

  "PublishStorageSyncMessage" should {
    "encode as expected by the EventBridge pipe" in {

      val encoded = message.asJson.noSpaces
      encoded shouldBe expectedEncodedMessage
    }

  }

  "SQSPublishStorageSyncMessenger" should {
    "send the expected message to the expected queue" in {
      val mockSqs = new MockSqsAsyncClient()
      val queueUrl = "http://example.com/queue/publish-storage-sync"

      val messenger = new SQSPublishStorageSyncMessenger(mockSqs, queueUrl)
      messenger.queueMessage(message).futureValue

      val actualMessage = mockSqs.sendMessageCalls.loneElement
      actualMessage.queueUrl() shouldBe queueUrl
      actualMessage
        .messageBody() shouldBe expectedEncodedMessage
    }
  }
}
