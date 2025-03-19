// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DoiRedirectSpec extends AnyWordSpec with Suite with Matchers {
  val publicUrl = "https://discover.pennsieve.org"
  val organizationId = 367
  val organizationName = "SPARC"
  val organizationUrl = "https://sparc.science"
  val organizationRedirectUrlFormat =
    s"${organizationUrl}/datasets/{{datasetId}}/version/{{versionId}}"
  val organizationReleaseRedirectUrlFormat =
    s"${organizationUrl}/code/{{datasetId}}/version/{{versionId}}"

  val datasetId = 4
  val versionId = 2

  val releaseId = 3
  val releaseVersion = 14

  lazy val defaultSettings = WorkspaceSettings.default(publicUrl)

  "DoiRedirect" should {
    "provide defaults" in {
      val doiRedirect = DoiRedirect(RedirectSettings(defaultSettings))

      val datasetUrl = doiRedirect.getDatasetUrl(datasetId, versionId)
      datasetUrl should startWith(publicUrl)
      datasetUrl should endWith(f"datasets/${datasetId}/version/${versionId}")

      val releaseUrl = doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseUrl should startWith(publicUrl)
      releaseUrl should endWith(f"code/${releaseId}/version/${releaseVersion}")
    }

    "provide workspace-specific dataset url" in {
      val workspaceSettings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat,
        redirectReleaseUrl = Some(organizationReleaseRedirectUrlFormat)
      )

      val doiRedirect = DoiRedirect(workspaceSettings + defaultSettings)

      val datasetUrl = doiRedirect.getDatasetUrl(datasetId, versionId)
      datasetUrl should startWith(organizationUrl)
      datasetUrl should endWith(f"datasets/${datasetId}/version/${versionId}")
    }

    "provide workspace-specific release url" in {
      val workspaceSettings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat,
        redirectReleaseUrl = Some(organizationReleaseRedirectUrlFormat)
      )

      val doiRedirect = DoiRedirect(workspaceSettings + defaultSettings)

      val releaseUrl = doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseUrl should startWith(organizationUrl)
      releaseUrl should endWith(f"code/${releaseId}/version/${releaseVersion}")
    }

    "use default redirect url for release when not specified in WorkspaceSettings" in {
      val workspaceSettings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat
      )

      val doiRedirect = DoiRedirect(workspaceSettings + defaultSettings)

      val releaseUrl = doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseUrl should startWith(publicUrl)
      releaseUrl should endWith(f"code/${releaseId}/version/${releaseVersion}")
    }
  }
}
