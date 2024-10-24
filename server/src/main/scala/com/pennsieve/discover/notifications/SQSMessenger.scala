// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.notifications

import com.pennsieve.discover.Ports
import com.pennsieve.service.utilities.LogContext
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

import scala.concurrent.{ ExecutionContext, Future }

object SQSMessenger {
  def queueMessage(
    queueUrl: String,
    message: SQSNotification
  )(implicit
    ec: ExecutionContext,
    logContext: LogContext,
    ports: Ports
  ): Future[Unit] = Future {
    ports.log.info(s"queueMessage() queuing message: ${message}")

    val sendMessageRequest = SendMessageRequest
      .builder()
      .queueUrl(queueUrl)
      .messageBody(message.asJson.toString)
      .delaySeconds(5)
      .build()

    ports.sqsClient.sendMessage(sendMessageRequest)
  }
}
