// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.db

import com.pennsieve.discover.ServiceSpecHarness
import com.pennsieve.discover.models.{ PublicDataset, WorkspaceSettings }
import com.pennsieve.test.AwaitableImplicits
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class WorkspaceSettingsMapperSpec
    extends AnyWordSpec
    with ServiceSpecHarness
    with AwaitableImplicits
    with Matchers {

  "WorkspaceSettingsTable" should {
    "enforce nulls not distinct in lookup key" in {
      val organizationId = 367
      val organizationName = "SPARC"
      val organizationRedirectUrlFormat =
        "https://sparc.science/datasets/{{datasetId}}/version/{{versionId}}"
      val organizationRedirectReleaseUrlFormat =
        "https://sparc.science/code/{{datasetId}}/version/{{versionId}}"

      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat,
        redirectReleaseUrl = Some(organizationRedirectReleaseUrlFormat)
      )

      // Add new settings
      ports.db.run(WorkspaceSettingsMapper.addSettings(settings)).await

      // second attempt to add settings with same org id should fail
      val exception = intercept[org.postgresql.util.PSQLException] {
        ports.db.run(WorkspaceSettingsMapper.addSettings(settings)).await
      }

      exception.getSQLState shouldBe "23505" // Unique violation in Postgres

    }
  }

  "WorkspaceSettingsMapper" should {
    "get None when no settings are defined" in {
      val settings = ports.db.run(WorkspaceSettingsMapper.getSettings(1)).await

      settings shouldBe None
    }

    "getSettings should return correct settings when no tag is present" in {
      val organizationId = 367
      val organizationName = "SPARC"
      val organizationRedirectUrlFormat =
        "https://sparc.science/datasets/{{datasetId}}/version/{{versionId}}"
      val organizationRedirectReleaseUrlFormat =
        "https://sparc.science/code/{{datasetId}}/version/{{versionId}}"

      val settings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl = organizationRedirectUrlFormat,
        redirectReleaseUrl = Some(organizationRedirectReleaseUrlFormat)
      )

      ports.db.run(WorkspaceSettingsMapper.addSettings(settings)).await

      val settingsMaybe =
        ports.db.run(WorkspaceSettingsMapper.getSettings(organizationId)).await

      val actualSettings = settingsMaybe.value
      actualSettings.organizationId shouldBe organizationId
      actualSettings.publisherName shouldBe organizationName
      actualSettings.redirectUrl shouldBe organizationRedirectUrlFormat
      actualSettings.redirectReleaseUrl shouldBe Some(
        organizationRedirectReleaseUrlFormat
      )
    }

    "getSettings return correct settings when tag is present" in {
      val organizationId = 676
      val organizationName = "Publishing Collections"

      val discoverSettings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl =
          "https://discover.pennsieve.io/collections/{{datasetId}}/version/{{versionId}}"
      )

      val sparcSettings = WorkspaceSettings(
        organizationId = organizationId,
        publisherName = organizationName,
        redirectUrl =
          "https://sparc.science/collections/{{datasetId}}/version/{{versionId}}",
        publisherTag = Some(s"${PublicDataset.publisherTagKey}:sparc")
      )

      ports.db
        .run(
          WorkspaceSettingsMapper
            .addSettings(discoverSettings) >> WorkspaceSettingsMapper
            .addSettings(sparcSettings)
        )
        .await

      val actualDiscoverSettings = ports.db
        .run(
          WorkspaceSettingsMapper.getSettings(organizationId = organizationId)
        )
        .await
        .value
      actualDiscoverSettings.organizationId shouldBe organizationId
      actualDiscoverSettings.publisherName shouldBe organizationName
      actualDiscoverSettings.redirectUrl shouldBe discoverSettings.redirectUrl
      actualDiscoverSettings.redirectReleaseUrl shouldBe None

      val actualSparcSettings = ports.db
        .run(
          WorkspaceSettingsMapper.getSettings(
            organizationId = organizationId,
            publisherTag = sparcSettings.publisherTag
          )
        )
        .await
        .value

      actualSparcSettings.organizationId shouldBe organizationId
      actualSparcSettings.publisherName shouldBe organizationName
      actualSparcSettings.redirectUrl shouldBe sparcSettings.redirectUrl
      actualSparcSettings.redirectReleaseUrl shouldBe None
    }

  }
}
