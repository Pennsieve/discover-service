// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.models.PackageType
import com.pennsieve.discover.client.definitions.DatasetsPage
import com.pennsieve.discover.{
  client,
  server,
  ServiceSpecHarness,
  TestUtilities
}
import com.pennsieve.discover.clients.MockSearchClient
import com.pennsieve.discover.client.search.{
  SearchClient,
  SearchDatasetsResponse,
  SearchFilesResponse,
  SearchRecordsResponse
}
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions.SponsorshipDto
import com.pennsieve.models.PublishStatus.{ EmbargoSucceeded, PublishSucceeded }
import com.pennsieve.models.DatasetMetadata._
import com.pennsieve.models.FileManifest
import com.pennsieve.models.FileType
import com.pennsieve.test.EitherValue._
import io.scalaland.chimney.dsl._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SearchHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def createRoutes(): Route =
    Route.seal(SearchHandler.routes(ports))

  def createClient(routes: Route): SearchClient =
    SearchClient.httpClient(Route.toFunction(routes))

  val searchClient: SearchClient = createClient(createRoutes())

  def toClientDefinition(
    dto: server.definitions.PublicDatasetDto
  ): client.definitions.PublicDatasetDto =
    dto
      .into[client.definitions.PublicDatasetDto]
      .transform

  "GET /search/datasets" should {

    "return a paged list of datasets matching the query terms" in {

      val randomDatasetName = TestUtilities.randomString()

      val publicDataset1 =
        TestUtilities.createDataset(ports.db)(
          name = randomDatasetName,
          sourceOrganizationId = 1,
          sourceDatasetId = 1
        )
      val publicDataset2 =
        TestUtilities.createDataset(ports.db)(
          name = "My Dataset",
          sourceOrganizationId = 1,
          sourceDatasetId = 2
        )
      val publicDataset3 =
        TestUtilities.createDataset(ports.db)(
          name = "Embargoed Dataset",
          sourceOrganizationId = 1,
          sourceDatasetId = 3
        )

      val publicDataset1_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishSucceeded
        )
      val publicDataset2_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset2.id,
          status = PublishSucceeded
        )
      val publicDataset3_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset3.id,
          status = EmbargoSucceeded
        )

      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = 1,
        version = publicDataset1_V1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val contributor2 = TestUtilities.createContributor(ports.db)(
        firstName = "John",
        lastName = "Malkovich",
        orcid = None,
        datasetId = publicDataset2.id,
        organizationId = 1,
        version = publicDataset2_V1.version,
        sourceContributorId = 2,
        sourceUserId = Some(2)
      )

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset1.id,
        version = publicDataset1_V1.version,
        sourceCollectionId = 1
      )

      ports.searchClient.asInstanceOf[MockSearchClient].datasetSearchResults +=
        DatasetDocument(
          publicDataset1,
          publicDataset1_V1,
          List(contributor),
          Readme("Dataset 1 version 1 readme"),
          None,
          None,
          List(collection),
          List.empty
        )
      ports.searchClient.asInstanceOf[MockSearchClient].datasetSearchResults +=
        DatasetDocument(
          publicDataset3,
          publicDataset3_V1,
          List(),
          Readme("Dataset 3 version 1 readme"),
          None,
          None,
          List.empty,
          List.empty
        )

      val expected = DatasetsPage(
        10,
        0,
        2L,
        Vector(
          toClientDefinition(
            PublicDatasetDTO(
              publicDataset1,
              publicDataset1_V1,
              Vector(PublicContributorDTO.apply(contributor)),
              None,
              None,
              Some(Vector(PublicCollectionDTO.apply(collection))),
              Some(Vector.empty),
              None,
              None
            )
          ),
          toClientDefinition(
            PublicDatasetDTO(
              publicDataset3,
              publicDataset3_V1,
              Vector(),
              None: Option[SponsorshipDto],
              None,
              Some(Vector.empty),
              Some(Vector.empty),
              None,
              None
            )
          )
        )
      )

      val response: DatasetsPage =
        searchClient
          .searchDatasets(query = Some(randomDatasetName))
          .awaitFinite()
          .value
          .asInstanceOf[SearchDatasetsResponse.OK]
          .value

      response shouldBe expected
    }

    "return 400 when query parameters are malformed" in {

      searchClient
        .searchDatasets(orderBy = Some("unknown-field"))
        .awaitFinite()
        .value shouldBe an[SearchDatasetsResponse.BadRequest]

      searchClient
        .searchDatasets(orderDirection = Some("horizontally"))
        .awaitFinite()
        .value shouldBe an[SearchDatasetsResponse.BadRequest]
    }
  }

  "GET /search/files" should {

    "return a paged list of files matching the query terms" in {

      val randomDatasetName = TestUtilities.randomString()

      val publicDataset1 =
        TestUtilities.createDataset(ports.db)(
          name = randomDatasetName,
          sourceOrganizationId = 1,
          sourceDatasetId = 1
        )

      val publicDataset1_V1 =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = publicDataset1.id,
          status = PublishSucceeded
        )

      val contributor = TestUtilities.createContributor(ports.db)(
        firstName = "Sally",
        lastName = "Fields",
        orcid = None,
        datasetId = publicDataset1.id,
        organizationId = 1,
        version = publicDataset1_V1.version,
        sourceContributorId = 1,
        sourceUserId = Some(1)
      )

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = publicDataset1.id,
        version = publicDataset1_V1.version,
        sourceCollectionId = 1
      )

      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .fileSearchResults +=
        FileDocument(
          FileManifest("brain.dcm", 12345L, FileType.DICOM, Some("123")),
          DatasetDocument(
            publicDataset1,
            publicDataset1_V1,
            List(contributor),
            Readme("Dataset 1 version 1 readme"),
            None,
            None,
            List(collection),
            List.empty
          )
        )

      val response = searchClient
        .searchFiles(fileType = Some("DICOM"))
        .awaitFinite()
        .value
        .asInstanceOf[SearchFilesResponse.OK]
        .value

      response.files.length shouldBe 1
      response.files.head.fileType shouldBe "DICOM"
      response.files.head.packageType shouldBe PackageType.MRI
      response.files.head.sourcePackageId shouldBe Some("123")

      response.totalCount shouldBe 1
      response.limit shouldBe 10
      response.offset shouldBe 0
    }
  }

  "GET /search/records" should {

    "return a paged list of records matching the query terms" in {
      val randomDatasetName = TestUtilities.randomString()

      val dataset =
        TestUtilities.createDataset(ports.db)(
          name = randomDatasetName,
          sourceOrganizationId = 1,
          sourceDatasetId = 1
        )

      val version =
        TestUtilities.createNewDatasetVersion(ports.db)(
          id = dataset.id,
          status = PublishSucceeded
        )

      ports.searchClient
        .asInstanceOf[MockSearchClient]
        .recordSearchResults ++= List(
        RecordDocument(
          Record(
            model = "medicine",
            datasetId = dataset.id,
            version = version.version,
            organizationName = dataset.sourceOrganizationName,
            properties = Map("name" -> "Aspirin")
          )
        ),
        RecordDocument(
          Record(
            model = "patient",
            datasetId = dataset.id,
            version = version.version,
            organizationName = dataset.sourceOrganizationName,
            properties = Map("name" -> "Joe")
          )
        )
      )

      val response = searchClient
        .searchRecords()
        .awaitFinite()
        .value
        .asInstanceOf[SearchRecordsResponse.OK]
        .value

      response.records.length shouldBe 2
      response.totalCount shouldBe 2
      response.limit shouldBe 10
      response.offset shouldBe 0
    }
  }
}
