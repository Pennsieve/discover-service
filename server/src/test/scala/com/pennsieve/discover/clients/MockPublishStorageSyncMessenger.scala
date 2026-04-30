// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import com.pennsieve.discover.notifications.{
  PublishStorageSyncMessage,
  PublishStorageSyncMessenger
}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockPublishStorageSyncMessenger extends PublishStorageSyncMessenger {
  val queuedMessages: ArrayBuffer[PublishStorageSyncMessage] = ArrayBuffer.empty

  override def queueMessage(
    message: PublishStorageSyncMessage
  )(implicit
    ec: ExecutionContext
  ): Future[String] = {
    queuedMessages += message
    Future.successful("mock-message-id")
  }

  def clear(): Unit = queuedMessages.clear()
}
