// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.Done
import com.pennsieve.discover.models.PublicDatasetVersion
import com.pennsieve.discover.notifications.SQSNotification
import com.pennsieve.service.utilities.LogContext

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockVictorOpsClient extends VictorOpsClient {

  val sentAlerts = ListBuffer.empty[VictorOpsAlert]

  def sendAlert(
    alert: VictorOpsAlert
  )(implicit
    logContext: LogContext
  ): Future[Done] = {
    sentAlerts += alert
    Future.successful(Done)
  }
}
