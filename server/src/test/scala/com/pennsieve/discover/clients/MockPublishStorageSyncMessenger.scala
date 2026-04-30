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
  private var failNextWith: Option[Throwable] = None

  /** Cause the next call to queueMessage to return Future.failed(t). */
  def failNext(t: Throwable): Unit = failNextWith = Some(t)

  override def queueMessage(
    message: PublishStorageSyncMessage
  )(implicit
    ec: ExecutionContext
  ): Future[String] = failNextWith match {
    case Some(t) =>
      failNextWith = None
      Future.failed(t)
    case None =>
      queuedMessages += message
      Future.successful("mock-message-id")
  }

  def clear(): Unit = {
    failNextWith = None
    queuedMessages.clear()
  }
}
