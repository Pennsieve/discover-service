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
  def createdAt = column[OffsetDateTime]("created_at")
  def updatedAt = column[OffsetDateTime]("updated_at")

  def * =
    (id, organizationId, publisherName, redirectUrl, createdAt, updatedAt)
      .mapTo[WorkspaceSettings]
}

object WorkspaceSettingsMapper
    extends TableQuery(new WorkspaceSettingsTable(_)) {
  def getSettings(
    organizationId: Int
  )(implicit
    executionContext: ExecutionContext
  ): DBIOAction[Option[WorkspaceSettings], NoStream, Effect.Read with Effect] =
    this
      .filter(_.organizationId === organizationId)
      .result
      .headOption
}
