// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model._

import java.util.concurrent.CompletableFuture
import scala.collection.mutable.ListBuffer

class MockSqsAsyncClient extends SqsAsyncClient {
  var sendMessageCalls: ListBuffer[SendMessageRequest] = ListBuffer.empty

  override def sendMessage(
    request: SendMessageRequest
  ): CompletableFuture[SendMessageResponse] = {
    sendMessageCalls = sendMessageCalls :+ request
    CompletableFuture.completedFuture(
      SendMessageResponse
        .builder()
        .messageId("test-message-id")
        .build()
    )
  }

  // Required methods
  override def serviceName(): String = "sqs"
  override def close(): Unit = ()

  // All other methods throw NotImplementedError
  private def notImplemented: Nothing =
    throw new NotImplementedError("Mock method not implemented")

  override def addPermission(
    request: AddPermissionRequest
  ): CompletableFuture[AddPermissionResponse] = notImplemented
  override def changeMessageVisibility(
    request: ChangeMessageVisibilityRequest
  ): CompletableFuture[ChangeMessageVisibilityResponse] = notImplemented
  override def changeMessageVisibilityBatch(
    request: ChangeMessageVisibilityBatchRequest
  ): CompletableFuture[ChangeMessageVisibilityBatchResponse] = notImplemented
  override def createQueue(
    request: CreateQueueRequest
  ): CompletableFuture[CreateQueueResponse] = notImplemented
  override def deleteMessage(
    request: DeleteMessageRequest
  ): CompletableFuture[DeleteMessageResponse] = notImplemented
  override def deleteMessageBatch(
    request: DeleteMessageBatchRequest
  ): CompletableFuture[DeleteMessageBatchResponse] =
    notImplemented
  override def deleteQueue(
    request: DeleteQueueRequest
  ): CompletableFuture[DeleteQueueResponse] = notImplemented
  override def getQueueAttributes(
    request: GetQueueAttributesRequest
  ): CompletableFuture[GetQueueAttributesResponse] =
    notImplemented
  override def getQueueUrl(
    request: GetQueueUrlRequest
  ): CompletableFuture[GetQueueUrlResponse] = notImplemented
  override def listDeadLetterSourceQueues(
    request: ListDeadLetterSourceQueuesRequest
  ): CompletableFuture[ListDeadLetterSourceQueuesResponse] = notImplemented
  override def listQueues(
    request: ListQueuesRequest
  ): CompletableFuture[ListQueuesResponse] = notImplemented
  override def listQueues(): CompletableFuture[ListQueuesResponse] =
    notImplemented
  override def listQueueTags(
    request: ListQueueTagsRequest
  ): CompletableFuture[ListQueueTagsResponse] = notImplemented
  override def purgeQueue(
    request: PurgeQueueRequest
  ): CompletableFuture[PurgeQueueResponse] = notImplemented
  override def receiveMessage(
    request: ReceiveMessageRequest
  ): CompletableFuture[ReceiveMessageResponse] = notImplemented
  override def removePermission(
    request: RemovePermissionRequest
  ): CompletableFuture[RemovePermissionResponse] =
    notImplemented
  override def sendMessageBatch(
    request: SendMessageBatchRequest
  ): CompletableFuture[SendMessageBatchResponse] =
    notImplemented
  override def setQueueAttributes(
    request: SetQueueAttributesRequest
  ): CompletableFuture[SetQueueAttributesResponse] =
    notImplemented
  override def tagQueue(
    request: TagQueueRequest
  ): CompletableFuture[TagQueueResponse] = notImplemented
  override def untagQueue(
    request: UntagQueueRequest
  ): CompletableFuture[UntagQueueResponse] = notImplemented
}
