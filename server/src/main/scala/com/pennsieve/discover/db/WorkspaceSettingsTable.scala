// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.models.WorkspaceSettings

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

final class WorkspaceSettingsTable(tag: Tag)
    extends Table[WorkspaceSettings](tag, "workspace_settings") {

  def id = column[Int]("id", O.PrimaryKey, O.AutoInc)
  def organizationId = column[Int]("organization_id")
  def publisherName = column[String]("publisher_name")
  def redirectUrl = column[String]("redirect_url")
  def redirectReleaseUrl = column[Option[String]]("redirect_release_url")
  def publisherTag = column[Option[String]]("publisher_tag")
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (
      id,
      organizationId,
      publisherName,
      redirectUrl,
      redirectReleaseUrl,
      publisherTag,
      createdAt,
      updatedAt
    ).mapTo[WorkspaceSettings]
}

object WorkspaceSettingsMapper
    extends TableQuery(new WorkspaceSettingsTable(_)) {

  def addSettings(
    settings: WorkspaceSettings
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[
    WorkspaceSettings,
    NoStream,
    Effect.Read with Effect.Write with Effect
  ] =
    (this returning this) += settings

  def getSettings(
    organizationId: Int,
    publisherTag: Option[String] = None
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[WorkspaceSettings], NoStream, Effect.Read with Effect] = {
    this
      .filter(_.organizationId === organizationId)
      .filter { ws =>
        publisherTag.fold(ws.publisherTag.isEmpty.?) { requestedTag =>
          ws.publisherTag === requestedTag
        }
      }
      .result
      .headOption
  }
}
