// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import cats.data.EitherT
import cats.implicits._
import com.pennsieve.discover.server.definitions.DatasetPublishStatus

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockPennsieveApiClient extends PennsieveApiClient {

  val publishCompleteRequests =
    ListBuffer.empty[(DatasetPublishStatus, Option[String])]

  def putPublishComplete(
    publishStatus: DatasetPublishStatus,
    error: Option[String] = None
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    publishCompleteRequests += ((publishStatus, error))
    EitherT.pure(())
  }

  val startReleaseRequests =
    ListBuffer.empty[(Int, Int)]

  def startRelease(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    startReleaseRequests += ((sourceOrganizationId, sourceDatasetId))

    if (sourceDatasetId == 999) // Magic number: Trigger error
      EitherT.leftT(HttpError(401, "Cannot start release"))
    else
      EitherT.pure(())
  }

  def getDataUseAgreement(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Option[DataUseAgreement]] =
    EitherT.rightT(
      Some(
        DataUseAgreement(id = 12, name = "Agreement #1", body = "Legal Text")
      )
    )

  def getDatasetPreviewers(
    sourceOrganizationId: Int,
    sourceDatasetId: Int
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, List[DatasetPreview]] = {
    EitherT.rightT(
      List(
        DatasetPreview(
          user = User("N:user:1", "Joe Schmo", 1),
          embargoAccess = "Requested"
        ),
        DatasetPreview(
          user = User("N:user:2", "Jane Schmo", 2),
          embargoAccess = "Granted"
        ),
        DatasetPreview(
          user = User("N:user:3", "Ivan Ivanov", 3),
          embargoAccess = "Refused"
        )
      )
    )
  }

  def clear(): Unit = {
    publishCompleteRequests.clear()
    startReleaseRequests.clear()
  }

  def requestPreview(
    organizationId: Int,
    previewRequest: APIPreviewAccessRequest
  )(implicit
    ec: ExecutionContext
  ): EitherT[Future, HttpError, Unit] = {
    if (organizationId == 999) {
      return EitherT.leftT(HttpError(404, "Organization doesn't exist"))
    }
    if (previewRequest.datasetId == 999) {
      return EitherT.leftT(HttpError(404, "Dataset doesn't exist"))
    }
    previewRequest.dataUseAgreementId match {
      case Some(999) =>
        EitherT.leftT(HttpError(404, "Dataset use agreement doesn't exist"))
      case _ => EitherT.pure(())
    }
  }
}
