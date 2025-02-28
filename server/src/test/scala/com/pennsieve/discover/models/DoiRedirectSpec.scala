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

  "DoiRedirect" should {
    "provide defaults" in {
      val doiRedirect = DoiRedirect(WorkspaceSettings.default(publicUrl))

      doiRedirect.getDatasetUrl(datasetId, versionId) should startWith(
        publicUrl
      )
    }

    "provide workspace specifics" in {
      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat
      )
      val doiRedirect = DoiRedirect(settings)

      doiRedirect.getDatasetUrl(datasetId, versionId) should startWith(
        organizationUrl
      )
    }

    "provide release redirect url" in {
      val doiRedirect = DoiRedirect(WorkspaceSettings.default(publicUrl))

      val releaseRedirectUrl =
        doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseRedirectUrl should startWith(publicUrl)
      releaseRedirectUrl should endWith(
        f"code/${releaseId}/version/${releaseVersion}"
      )
    }

    "use dataset redirect url for release when not specified" in {
      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat
      )
      val doiRedirect = DoiRedirect(settings)

      val releaseRedirectUrl =
        doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseRedirectUrl should startWith(organizationUrl)
      releaseRedirectUrl should endWith(
        f"datasets/${releaseId}/version/${releaseVersion}"
      )
    }

    "use release redirect url for release when specified" in {
      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat,
        redirectReleaseUrl = Some(organizationReleaseRedirectUrlFormat)
      )
      val doiRedirect = DoiRedirect(settings)

      val releaseRedirectUrl =
        doiRedirect.getReleaseUrl(releaseId, releaseVersion)
      releaseRedirectUrl should startWith(organizationUrl)
      releaseRedirectUrl should endWith(
        f"code/${releaseId}/version/${releaseVersion}"
      )
    }
  }
}
