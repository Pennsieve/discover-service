// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.server.definitions.PublicDatasetDto
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

    "fail to decode a manifest.json missing contributors" in {
      val jsonString =
        s"""
           |{
           |  "datePublished": "2024-11-04",
           |  "pennsieveDatasetId": 5168,
           |  "version": 1,
           |  "name": "muftring/test-github-publishing-manual",
           |  "description": "a test repo to test the manual publishing of this code repo",
           |  "creator": {
           |    "id": 0,
           |    "first_name": "Michael",
           |    "last_name": "Uftring",
           |    "degree": "M.S.",
           |    "orcid": "0000-0001-7054-4685"
           |  },
           |  "sourceOrganization": "Publishing 5.0 Workspace",
           |  "keywords": [],
           |  "license": "MIT License",
           |  "@id": "10.21397/mo22-xgzv",
           |  "publisher": "The University of Pennsylvania",
           |  "@context": "http://schema.org/",
           |  "@type": "Release",
           |  "schemaVersion": "http://schema.org/version/3.7/",
           |  "files": [
           |    {
           |      "name": "changelog.md",
           |      "path": "changelog.md",
           |      "size": 244,
           |      "fileType": "Markdown",
           |      "s3VersionId": "WYOGVhtNTYcdw81VRUXmLcoGj_bM7rSF"
           |    },
           |    {
           |      "name": "readme.md",
           |      "path": "readme.md",
           |      "size": 286,
           |      "fileType": "Markdown",
           |      "s3VersionId": "dS9fC3I4q7OfrPbxSYjYRXFi2t3d1mwy"
           |    },
           |    {
           |      "name": "muftring-test-github-publishing-manual-v1.0.2-0-g3931565.zip",
           |      "path": "assets/muftring-test-github-publishing-manual-v1.0.2-0-g3931565.zip",
           |      "size": 8586,
           |      "fileType": "ZIP"
           |    },
           |    {
           |      "name": "manifest.json",
           |      "path": "manifest.json",
           |      "size": 1947,
           |      "fileType": "Json"
           |    }
           |  ],
           |  "release": {
           |    "origin": "GitHub",
           |    "url": "https://github.com/muftring/test-github-publishing-manual",
           |    "label": "v1.0.2",
           |    "marker": "3931565f392628e48c4158f8262a6e728207cb84"
           |  },
           |  "pennsieveSchemaVersion": "5.0"
           |}
           |""".stripMargin

      val decoded = decode[DatasetMetadata](jsonString)
      decoded.isRight shouldBe false
    }

    "decode release-asset-listing.json" in {
      val jsonString =
        s"""
           |{
           |  "files": [
           |    {
           |      "file": "LICENSE",
           |      "name": "LICENSE",
           |      "type": "file",
           |      "size": 1072
           |    },
           |    {
           |      "file": "README.md",
           |      "name": "README.md",
           |      "type": "file",
           |      "size": 286
           |    },
           |    {
           |      "file": "code/",
           |      "name": "code",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "code/graph.py",
           |      "name": "graph.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/main.py",
           |      "name": "main.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/package.py",
           |      "name": "package.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/reporting.py",
           |      "name": "reporting.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "code/testing.py",
           |      "name": "testing.py",
           |      "type": "file",
           |      "size": 1024
           |    },
           |    {
           |      "file": "data/",
           |      "name": "data",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "data/patient.dat",
           |      "name": "patient.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/sample.dat",
           |      "name": "sample.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/study.dat",
           |      "name": "study.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "data/visit.dat",
           |      "name": "visit.dat",
           |      "type": "file",
           |      "size": 1048576
           |    },
           |    {
           |      "file": "model/",
           |      "name": "model",
           |      "type": "folder",
           |      "size": 0
           |    },
           |    {
           |      "file": "model/metadata.json",
           |      "name": "metadata.json",
           |      "type": "file",
           |      "size": 582
           |    }
           |  ]
           |}
           |
           |""".stripMargin

      val decoded = decode[ReleaseAssetListing](jsonString)
      decoded.isRight shouldBe true
      val listing = decoded.toOption.get
      listing.files.length shouldEqual 15
    }

    "decode a dataset into a DatasetDTO" in {
      val jsonString =
        s"""
           |{
           |    "id": 5160,
           |    "sourceDatasetId": 86,
           |    "name": "test-publishing-2024-10-10-a",
           |    "description": "test",
           |    "ownerId": 177,
           |    "ownerFirstName": "Michael",
           |    "ownerLastName": "Uftring",
           |    "ownerOrcid": "0000-0001-7054-4685",
           |    "organizationName": "Publishing 5.0 Workspace",
           |    "organizationId": 39,
           |    "license": "Community Data License Agreement – Permissive",
           |    "tags": [
           |        "publishing"
           |    ],
           |    "version": 1,
           |    "revision": null,
           |    "size": 5087,
           |    "modelCount": [],
           |    "fileCount": 5,
           |    "recordCount": 0,
           |    "uri": "s3://pennsieve-dev-discover-publish50-use1/5160/",
           |    "arn": "arn:aws:s3:::pennsieve-dev-discover-publish50-use1/5160/",
           |    "status": "PUBLISH_SUCCEEDED",
           |    "doi": "10.21397/y2oo-9twc",
           |    "banner": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/banner.jpg",
           |    "readme": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/readme.md",
           |    "changelog": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/changelog.md",
           |    "contributors": [
           |        {
           |            "firstName": "Michael",
           |            "middleInitial": null,
           |            "lastName": "Uftring",
           |            "degree": null,
           |            "orcid": "0000-0001-7054-4685"
           |        }
           |    ],
           |    "collections": [],
           |    "externalPublications": [],
           |    "sponsorship": null,
           |    "pennsieveSchemaVersion": "4.0",
           |    "createdAt": "2024-10-10T15:55:10.785499Z",
           |    "updatedAt": "2024-10-10T15:59:48.854633Z",
           |    "firstPublishedAt": "2024-10-10T15:55:10.771915Z",
           |    "versionPublishedAt": "2024-10-10T15:55:10.785499Z",
           |    "revisedAt": null,
           |    "embargo": false,
           |    "embargoReleaseDate": null,
           |    "embargoAccess": null,
           |    "datasetType": "research",
           |    "release": null
           |}
           |""".stripMargin

      val decoded = decode[PublicDatasetDto](jsonString)
      decoded.isRight shouldBe true
    }

    "decode a dataset missing datasetType into a DatasetDto" in {
      val jsonString =
        s"""
           |{
           |    "id": 5160,
           |    "sourceDatasetId": 86,
           |    "name": "test-publishing-2024-10-10-a",
           |    "description": "test",
           |    "ownerId": 177,
           |    "ownerFirstName": "Michael",
           |    "ownerLastName": "Uftring",
           |    "ownerOrcid": "0000-0001-7054-4685",
           |    "organizationName": "Publishing 5.0 Workspace",
           |    "organizationId": 39,
           |    "license": "Community Data License Agreement – Permissive",
           |    "tags": [
           |        "publishing"
           |    ],
           |    "version": 1,
           |    "revision": null,
           |    "size": 5087,
           |    "modelCount": [],
           |    "fileCount": 5,
           |    "recordCount": 0,
           |    "uri": "s3://pennsieve-dev-discover-publish50-use1/5160/",
           |    "arn": "arn:aws:s3:::pennsieve-dev-discover-publish50-use1/5160/",
           |    "status": "PUBLISH_SUCCEEDED",
           |    "doi": "10.21397/y2oo-9twc",
           |    "banner": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/banner.jpg",
           |    "readme": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/readme.md",
           |    "changelog": "https://assets.discover.pennsieve.net/dataset-assets/5160/1/changelog.md",
           |    "contributors": [
           |        {
           |            "firstName": "Michael",
           |            "middleInitial": null,
           |            "lastName": "Uftring",
           |            "degree": null,
           |            "orcid": "0000-0001-7054-4685"
           |        }
           |    ],
           |    "collections": [],
           |    "externalPublications": [],
           |    "sponsorship": null,
           |    "pennsieveSchemaVersion": "4.0",
           |    "createdAt": "2024-10-10T15:55:10.785499Z",
           |    "updatedAt": "2024-10-10T15:59:48.854633Z",
           |    "firstPublishedAt": "2024-10-10T15:55:10.771915Z",
           |    "versionPublishedAt": "2024-10-10T15:55:10.785499Z",
           |    "revisedAt": null,
           |    "embargo": false,
           |    "embargoReleaseDate": null,
           |    "embargoAccess": null
           |}
           |""".stripMargin

      val decoded = decode[PublicDatasetDto](jsonString)
      decoded.isRight shouldBe true
    }

    "decode a release result V5.0" in {
      val jsonString =
        s"""
           |[
           |  {
           |    "source_bucket": "embargo-bucket",
           |    "source_key": "5125/files/data/source-0.dat",
           |    "source_size": "4742331076932",
           |    "source_version_id": "Yu226jh9HZEZuPMm",
           |    "source_etag": "IAOqXjhaztsG",
           |    "source_sha256": "kp3MfoV3kWX2DO4uqOYj8hKo",
           |    "target_bucket": "publish-bucket",
           |    "target_key": "5125/files/data/source-0.dat",
           |    "target_size": "4742331076932",
           |    "target_version_id": "TeaY1sV7Vo000CKI",
           |    "target_etag": "OEQR4Fn4JEis",
           |    "target_sha256": "dr7bO3eVqee2jQvgnkEqqs8Q"
           |  },
           |  {
           |    "source_bucket": "embargo-bucket",
           |    "source_key": "5125/files/data/source-1.dat",
           |    "source_size": "4178022176514",
           |    "source_version_id": "1x76Keaufom9JsDr",
           |    "source_etag": "Ybc9SKjarN0R",
           |    "source_sha256": "2kvjORLN622LXoklC1jlqFsX",
           |    "target_bucket": "publish-bucket",
           |    "target_key": "5125/files/data/source-1.dat",
           |    "target_size": "4178022176514",
           |    "target_version_id": "V2dsjBzIDH6JZJ1q",
           |    "target_etag": "w4Lymba4XTHr",
           |    "target_sha256": "MMK6o4NKW0uSVsLlssvs9Anp"
           |  },
           |  {
           |    "source_bucket": "embargo-bucket",
           |    "source_key": "5125/files/data/source-2.dat",
           |    "source_size": "1187153020914",
           |    "source_version_id": "aNvPuGvNgJH2NNkB",
           |    "source_etag": "EG4sjAuYnmHJ",
           |    "source_sha256": "k8stpDnQaoI5zHwLyPCtSBQb",
           |    "target_bucket": "publish-bucket",
           |    "target_key": "5125/files/data/source-2.dat",
           |    "target_size": "1187153020914",
           |    "target_version_id": "eskZ6l7fA1CqUbDZ",
           |    "target_etag": "IrlOHsEVYGON",
           |    "target_sha256": "w5VmUCQ5WHs7wwNwRflcpCYB"
           |  },
           |  {
           |    "source_bucket": "embargo-bucket",
           |    "source_key": "5125/files/data/source-3.dat",
           |    "source_size": "4009136485934",
           |    "source_version_id": "I2KXz59q3S71ZA2E",
           |    "source_etag": "POKIH8x3gvM3",
           |    "source_sha256": "DVt9Dw8cI9b0gkRE8ioRmAfE",
           |    "target_bucket": "publish-bucket",
           |    "target_key": "5125/files/data/source-3.dat",
           |    "target_size": "4009136485934",
           |    "target_version_id": "fdmZRK9gw9SWR18x",
           |    "target_etag": "nrzBmBt1E6oW",
           |    "target_sha256": "s7o2bslcQANHkES64rwBy3gK"
           |  },
           |  {
           |    "source_bucket": "embargo-bucket",
           |    "source_key": "5125/files/data/source-4.dat",
           |    "source_size": "78740955173",
           |    "source_version_id": "es27PWaErvRWWADf",
           |    "source_etag": "SebTFT8lNdp1",
           |    "source_sha256": "NcEayL0VMeLWIb2bkMNGC3c1",
           |    "target_bucket": "publish-bucket",
           |    "target_key": "5125/files/data/source-4.dat",
           |    "target_size": "78740955173",
           |    "target_version_id": "funOSyUdSYYox51R",
           |    "target_etag": "WIfHl2y3qrCG",
           |    "target_sha256": "nR2Of7CBX7FsjzkyzgTFRqIU"
           |  }
           |]
           |""".stripMargin

      val decoded = decode[List[ReleaseActionV50]](jsonString)
      decoded.isRight shouldBe true
    }
  }
}
