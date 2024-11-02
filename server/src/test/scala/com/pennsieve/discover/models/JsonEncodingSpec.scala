// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.models.{
  DatasetMetadata,
  DatasetMetadataV5_0,
  Degree,
  FileManifest,
  FileType,
  License,
  PublishedContributor,
  ReleaseMetadataV5_0
}
import org.scalatest.Suite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.circe.parser.decode

class JsonEncodingSpec extends AnyWordSpec with Suite with Matchers {
  "JSON Encoding" should {
    "decode a ReleaseAssetListing" in {
      val jsonString =
        s"""
           |{"files":[{"file":"manifest.json","name":"manifest.json","type":"file","size":1234},{"file":"metadata.json","name":"metadata.json","type":"file","size":2345}]}
           |""".stripMargin

      val expected = ReleaseAssetListing(
        files = Seq(
          ReleaseAssetFile(
            file = "manifest.json",
            name = "manifest.json",
            `type` = ReleaseAssetFileType.File,
            size = 1234
          ),
          ReleaseAssetFile(
            file = "metadata.json",
            name = "metadata.json",
            `type` = ReleaseAssetFileType.File,
            size = 2345
          )
        )
      )
      val decoded = decode[ReleaseAssetListing](jsonString)
        .fold(l => (), r => r)

      decoded shouldEqual expected
    }

    "decode a manifest.json into a DatasetMetadata" in {
      val jsonString =
        s"""
           |{
           |    "datePublished": "2024-11-02",
           |    "pennsieveDatasetId": 9001,
           |    "version": 1,
           |    "name": "pennsieve/test-repo",
           |    "description": "test GitHub repo",
           |    "creator": {
           |        "id": 0,
           |        "first_name": "Code",
           |        "last_name": "Writer",
           |        "degree": "Ph.D.",
           |        "orcid": "0000-0001-0032-9105"
           |    },
           |    "contributors": [
           |        {
           |            "id": 0,
           |            "first_name": "Code",
           |            "last_name": "Writer",
           |            "degree": "Ph.D.",
           |            "orcid": "0000-0001-0032-9105"
           |        },
           |        {
           |            "id": 0,
           |            "first_name": "Data",
           |            "last_name": "Analyst",
           |            "degree": "M.S.",
           |            "orcid": "0001-0001-4404-8241"
           |        }
           |    ],
           |    "sourceOrganization": "Organization 39",
           |    "keywords": [],
           |    "license": "MIT License",
           |    "@id": "10.3301/dfgh-imlk",
           |    "publisher": "The University of Pennsylvania",
           |    "@context": "http://schema.org/",
           |    "@type": "Release",
           |    "schemaVersion": "http://schema.org/version/3.7/",
           |    "files": [
           |        {
           |            "name": "readme.md",
           |            "path": "readme.md",
           |            "size": 1234,
           |            "fileType": "Markdown",
           |            "s3VersionId": "5dd770adbfd44533a044d4c5a59d7488",
           |            "sha256": "fd00b4539afa45688a6b03dcbeb358de"
           |        },
           |        {
           |            "name": "changelog.md",
           |            "path": "changelog.md",
           |            "size": 2345,
           |            "fileType": "Markdown",
           |            "s3VersionId": "ae53f6038bdc46e687bbdcc6c96dbdff",
           |            "sha256": "83fdd04bc4c34e8891d51eb492ba9378"
           |        },
           |        {
           |            "name": "release.zip",
           |            "path": "assets/release.zip",
           |            "size": 156209,
           |            "fileType": "ZIP",
           |            "s3VersionId": "66f07f653f6d423cac595d1ec9e1afa3",
           |            "sha256": "084b587ed51c4094a0e7a63a69abdd17"
           |        },
           |        {
           |            "name": "manifest.json",
           |            "path": "manifest.json",
           |            "size": 0,
           |            "fileType": "Markdown"
           |        }
           |    ],
           |    "release": {
           |        "origin": "GitHub",
           |        "url": "https://github.com/pennsieve/test-repo",
           |        "label": "v1.0.0",
           |        "marker": "4911a0719a764bc68998b3b7481033aa"
           |    },
           |    "pennsieveSchemaVersion": "5.0"
           |}
           |
           |""".stripMargin

      val expected = DatasetMetadataV5_0(
        pennsieveDatasetId = 9001,
        version = 1,
        revision = None,
        name = "pennsieve/test-repo",
        description = "test GitHub repo",
        creator = PublishedContributor(
          first_name = "Code",
          last_name = "Writer",
          orcid = Some("0000-0001-0032-9105"),
          middle_initial = None,
          degree = Some(Degree.PhD)
        ),
        contributors = List(
          PublishedContributor(
            first_name = "Code",
            last_name = "Writer",
            orcid = Some("0000-0001-0032-9105"),
            middle_initial = None,
            degree = Some(Degree.PhD)
          ),
          PublishedContributor(
            first_name = "Data",
            last_name = "Analyst",
            orcid = Some("0001-0001-4404-8241"),
            middle_initial = None,
            degree = Some(Degree.MS)
          )
        ),
        sourceOrganization = "Organization 39",
        keywords = List.empty,
        datePublished = java.time.LocalDate.of(2024, 11, 2),
        license = Some(License.`MIT License`),
        `@id` = "10.3301/dfgh-imlk",
        publisher = "The University of Pennsylvania",
        `@context` = "http://schema.org/",
        `@type` = "Release",
        schemaVersion = "http://schema.org/version/3.7/",
        files = List(
          FileManifest(
            name = "readme.md",
            path = "readme.md",
            size = 1234,
            fileType = FileType.Markdown,
            s3VersionId = Some("5dd770adbfd44533a044d4c5a59d7488"),
            sha256 = Some("fd00b4539afa45688a6b03dcbeb358de")
          ),
          FileManifest(
            name = "changelog.md",
            path = "changelog.md",
            size = 2345,
            fileType = FileType.Markdown,
            s3VersionId = Some("ae53f6038bdc46e687bbdcc6c96dbdff"),
            sha256 = Some("83fdd04bc4c34e8891d51eb492ba9378")
          ),
          FileManifest(
            name = "release.zip",
            path = "assets/release.zip",
            size = 156209,
            fileType = FileType.ZIP,
            s3VersionId = Some("66f07f653f6d423cac595d1ec9e1afa3"),
            sha256 = Some("084b587ed51c4094a0e7a63a69abdd17")
          ),
          FileManifest(
            name = "manifest.json",
            path = "manifest.json",
            size = 0,
            fileType = FileType.Markdown
          )
        ),
        release = Some(
          ReleaseMetadataV5_0(
            origin = "GitHub",
            url = "https://github.com/pennsieve/test-repo",
            label = "v1.0.0",
            marker = "4911a0719a764bc68998b3b7481033aa"
          )
        ),
        pennsieveSchemaVersion = "5.0"
      )
      val decoded = decode[DatasetMetadata](jsonString)
        .fold(l => (), r => r)
      decoded shouldEqual expected
    }
  }
}
