// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import java.time.{ OffsetDateTime, ZoneOffset }

case class RedirectSettings(
  publisherName: String,
  redirectUrl: String,
  redirectReleaseUrl: String
)

object RedirectSettings {
  def apply(defaults: WorkspaceSettings): RedirectSettings = RedirectSettings(
    publisherName = defaults.publisherName,
    redirectUrl = defaults.redirectUrl,
    redirectReleaseUrl =
      defaults.redirectReleaseUrl.getOrElse(WorkspaceSettings.defaultUrl)
  )
}

case class WorkspaceSettings(
  id: Int = 0,
  organizationId: Int,
  publisherName: String,
  redirectUrl: String,
  redirectReleaseUrl: Option[String] = None,
  createdAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC),
  updatedAt: OffsetDateTime = OffsetDateTime.now(ZoneOffset.UTC)
) {
  def +(defaults: WorkspaceSettings): RedirectSettings =
    RedirectSettings(
      publisherName = publisherName,
      redirectUrl = redirectUrl,
      redirectReleaseUrl = redirectReleaseUrl.getOrElse(
        defaults.redirectReleaseUrl.getOrElse(WorkspaceSettings.defaultUrl)
      )
    )
}

object WorkspaceSettings {
  def defaultPublisher = "Pennsieve Discover"
  def defaultUrl = "https://discover.pennsieve.io"
  def default(publicUrl: String): WorkspaceSettings =
    WorkspaceSettings(
      organizationId = 0,
      publisherName = defaultPublisher,
      redirectUrl = s"${publicUrl}/datasets/{{datasetId}}/version/{{versionId}}",
      redirectReleaseUrl =
        Some(s"${publicUrl}/code/{{datasetId}}/version/{{versionId}}")
    )

  val tupled = (this.apply _).tupled
}
