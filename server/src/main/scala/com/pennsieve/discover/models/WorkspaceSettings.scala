// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class WorkspaceSettings(
  id: Int = 0,
  organizationId: Int,
  publisherName: String,
  redirectUrl: String,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
)

object WorkspaceSettings {
  def default(publicUrl: String): WorkspaceSettings =
    WorkspaceSettings(
      organizationId = 0,
      publisherName = "Pennsieve",
      redirectUrl = s"${publicUrl}/datasets/{{datasetId}}/version/{{versionId}}"
    )

  val tupled = (this.apply _).tupled
}
