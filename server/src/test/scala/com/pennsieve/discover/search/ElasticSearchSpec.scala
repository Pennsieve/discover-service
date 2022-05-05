// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.search

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.pennsieve.models._
import com.pennsieve.discover._
import com.pennsieve.discover.TestUtilities._
import com.pennsieve.discover.clients.AwsElasticSearchClient
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.Response
import io.circe.generic.auto._
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.RefreshPolicy
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.compat.java8.DurationConverters._
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class ElasticSearchSpec
    extends AnyWordSpec
    with Matchers
    with ServiceSpecHarness
    with DockerElasticSearchService {

  implicit lazy private val system: ActorSystem =
    ActorSystem("discover-service")
  implicit lazy private val executionContext: ExecutionContext =
    system.dispatcher

  var searchClient: AwsElasticSearchClient = _
  var searchPorts: Ports = _

  /**
    * Create a local Ports instance layered with a non-mock ElasticSearch client.
    */
  def getSearchPorts(config: Config): Ports = {
    val searchConfig =
      config.copy(awsElasticSearch = elasticSearchConfiguration)
    val searchPorts = getPorts(searchConfig)

    val searchClient = new AwsElasticSearchClient(
      elasticSearchConfiguration.elasticUri,
      searchConfig,
      RefreshPolicy.WaitFor
    )
    searchPorts.copy(searchClient = searchClient)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    searchPorts = getSearchPorts(config)
    searchClient = searchPorts.searchClient.asInstanceOf[AwsElasticSearchClient]
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    Search
      .buildSearchIndex(searchPorts)
      .awaitFinite(10.seconds)
  }

  def datasetDocument(
    id: Int = Random.nextInt(),
    sourceDatasetId: Int = Random.nextInt(),
    version: Int = Random.nextInt(),
    name: String = "Colors",
    description: String = "red green blue ganglia",
    ownerId: Int = 1,
    ownerFirstName: String = "Fynn",
    ownerLastName: String = "Blackwell",
    ownerOrcid: String = "0000-0001-2345-6789",
    organizationName: String = "University of Pennsylvania",
    organizationId: Int = 1,
    license: License = License.MIT,
    size: Long = 12345L,
    doi: String = "10.21397/abcd-1234",
    tags: Vector[String] = Vector("research"),
    modelCount: Vector[definitions.ModelCount] =
      Vector.empty[definitions.ModelCount],
    fileCount: Int = 10,
    recordCount: Int = 0,
    status: PublishStatus = PublishStatus.PublishSucceeded,
    readme: Option[String] = None,
    banner: Option[String] = None,
    createdAt: OffsetDateTime = OffsetDateTime.now(),
    updatedAt: OffsetDateTime = OffsetDateTime.now(),
    readmeContents: Readme = Readme("readme"),
    contributors: Seq[definitions.PublicContributorDto] = Seq.empty
  ): DatasetDocument = DatasetDocument(
    definitions.PublicDatasetDto(
      id = id,
      sourceDatasetId = Some(sourceDatasetId),
      version = version,
      name = name,
      description = description,
      ownerId = Some(ownerId),
      ownerFirstName = ownerFirstName,
      ownerLastName = ownerLastName,
      ownerOrcid = ownerOrcid,
      organizationName = organizationName,
      organizationId = Some(organizationId),
      license = license,
      size = size,
      doi = doi,
      tags = tags,
      modelCount = modelCount,
      fileCount = fileCount,
      recordCount = recordCount,
      uri = s"s3://discover/$id/$version/",
      arn = s"arn:aws:s3:::discover/$id/$version/",
      status = status,
      readme = readme,
      banner = banner,
      createdAt = createdAt,
      updatedAt = updatedAt,
      contributors = contributors.toVector
    ),
    readmeContents
  )

  val dataset1 = datasetDocument(
    name = "Colors",
    description = "red green blue ganglia",
    ownerFirstName = "Fynn",
    ownerLastName = "Blackwell",
    ownerOrcid = "0000-0001-2345-6789",
    organizationName = "University of Pennsylvania",
    doi = "10.21397/abcd-1234",
    size = 100L,
    tags = Vector("research"),
    createdAt = OffsetDateTime.now(),
    updatedAt = OffsetDateTime.now(),
    readmeContents = Readme("readme for dataset 1"),
    contributors = Seq(
      definitions
        .PublicContributorDto(
          firstName = "Sally",
          lastName = "Field",
          middleInitial = Some("M")
        )
    )
  )

  val dataset2 = datasetDocument(
    name = "PPMI",
    description = "neuro brain ganglion",
    ownerFirstName = "Fynn",
    ownerLastName = "Blackwell",
    ownerOrcid = "0000-0001-2345-6789",
    organizationName = "SPARC",
    organizationId = 10,
    doi = "10.21397/wxyz-9876",
    size = 200L,
    tags = Vector("research", "seizure"),
    createdAt = OffsetDateTime.now().plus(1.day.toJava),
    updatedAt = OffsetDateTime.now().plus(1.day.toJava),
    readmeContents = Readme("something else for dataset 2"),
    contributors = Seq(
      definitions
        .PublicContributorDto(
          firstName = "Jeffrey",
          lastName = "Laurance Ardell",
          degree = Some(Degree.MD)
        )
    )
  )

  val dataset3 = datasetDocument(
    name = "EEG TUSZ",
    description = "Test EEG dataset",
    ownerFirstName = "Forrest",
    ownerLastName = "Gump",
    ownerOrcid = "9999-9876-6767-3333",
    organizationName = "SPARC",
    organizationId = 10,
    doi = "10.21397/mmmm-1243",
    size = 300L,
    tags = Vector("eeg", "brain", "seizure"),
    createdAt = OffsetDateTime.now().plus(2.day.toJava),
    updatedAt = OffsetDateTime.now().plus(2.day.toJava),
    readmeContents = Readme("readme for dataset 3"),
    contributors = Seq(
      definitions
        .PublicContributorDto(
          firstName = "Colleen",
          lastName = "Clancy",
          middleInitial = Some("E"),
          degree = Some(Degree.PhD)
        )
    )
  )

  val datasets = Seq(dataset1, dataset2, dataset3)

  "search" should {

    "search all fields" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(Some("ppmi"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)
    }

    "handle typos" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(Some("brian~"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2, dataset3)
    }

    "search datasets by the contents of the dataset readme" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(query = Some("readme"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1, dataset3)
    }

    "search datasets by organization" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(organization = Some("SPARC"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2, dataset3)
    }

    "search datasets by organization id" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(organizationId = Some(10))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2, dataset3)
    }

    "search datasets for common stems" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(query = Some("researching"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1, dataset2)

      searchClient
        .searchDatasets(query = Some("ganglia~"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1, dataset2)
    }

    "search datasets by name" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(query = Some("color"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1)
    }

    "search datasets by tag" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(tags = Some(List("research")))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1, dataset2)

      searchClient
        .searchDatasets(tags = Some(List("seizure")))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2, dataset3)

      // AND multiple tags
      searchClient
        .searchDatasets(tags = Some(List("seizure", "research")))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      // normalize letter case
      searchClient
        .searchDatasets(tags = Some(List("SEIZURE")))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2, dataset3)

      // strict match
      searchClient
        .searchDatasets(tags = Some(List("seize")))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe empty
    }

    "search datasets by contributor" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(query = Some("Sally"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1)

      searchClient
        .searchDatasets(query = Some("Ardell"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      searchClient
        .searchDatasets(query = Some("Jeffrey Laurance Ardell"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      searchClient
        .searchDatasets(query = Some("Gump 9999"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset3)
    }

    "search datasets with structured query" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(query = Some("seizure | colors"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1, dataset2, dataset3)

      searchClient
        .searchDatasets(query = Some("seizure & colors"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe empty

      searchClient
        .searchDatasets(query = Some("seizure & PPMI"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      searchClient
        .searchDatasets(query = Some("(colors | PPMI) & neuro"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      searchClient
        .searchDatasets(query = Some("research +readme"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset1)

      searchClient
        .searchDatasets(query = Some("research -readme"))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe Set(dataset2)

      searchClient
        .searchDatasets(query = Some(""" "red blue" """))
        .awaitFinite()
        .datasets
        .to(Set) shouldBe empty
    }

    "handle limit and offset for datasets" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      val result1 = searchClient
        .searchDatasets(limit = 1, offset = 0)
        .awaitFinite()
        .datasets

      result1.length shouldBe 1

      val result2 = searchClient
        .searchDatasets(limit = 1, offset = 1)
        .awaitFinite()
        .datasets

      result2.length shouldBe 1

      result1 should !==(result2)

      val result3 = searchClient
        .searchDatasets(offset = 2)
        .awaitFinite()
        .datasets

      result3.length shouldBe 1
    }

    "order datasets by date created" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Date,
          orderDirection = OrderDirection.Ascending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset1, dataset2, dataset3)

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Date,
          orderDirection = OrderDirection.Descending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset3, dataset2, dataset1)
    }

    "order datasets by name" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Name,
          orderDirection = OrderDirection.Ascending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset1, dataset3, dataset2)

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Name,
          orderDirection = OrderDirection.Descending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset2, dataset3, dataset1)
    }

    "order datasets by name case-insensitively" in {
      val dataset4 = datasetDocument(name = "a lowercase name")
      val dataset5 = datasetDocument(name = "AN UPPERCASE NAME")
      val dataset6 = datasetDocument(name = "Some other capitalized dataset")

      searchClient
        .insertDatasets(List(dataset4, dataset5, dataset6))
        .awaitFinite()

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Name,
          orderDirection = OrderDirection.Ascending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset4, dataset5, dataset6)
    }

    "order datasets by size" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Size,
          orderDirection = OrderDirection.Ascending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset1, dataset2, dataset3)

      searchClient
        .searchDatasets(
          orderBy = OrderBy.Size,
          orderDirection = OrderDirection.Descending
        )
        .awaitFinite()
        .datasets shouldBe List(dataset3, dataset2, dataset1)
    }

    "replace existing document when adding a new version of a dataset" in {
      searchClient.insertDatasets(datasets).awaitFinite()

      searchClient
        .insertDataset(
          dataset1.copy(
            dataset = dataset1.dataset.copy(description = "yellow green blue")
          )
        )
        .awaitFinite()

      // Now "red" is not found anywhere
      searchClient
        .searchDatasets(Some("red"))
        .awaitFinite()
        .datasets shouldEqual Seq.empty

      // ... but "yellow" is
      searchClient
        .searchDatasets(Some("yellow"))
        .awaitFinite()
        .datasets
        .length shouldBe 1
    }

    "loading from the database should create a new index aliased to 'dataset'" in {
      val dataset1 =
        createDatasetV1(searchPorts.db)(status = PublishStatus.PublishSucceeded)
      createFile(searchPorts.db)(
        dataset1,
        path = "files/brain.dcm",
        fileType = "DICOM"
      )

      Search
        .buildSearchIndex(searchPorts)
        .awaitFinite()

      val aliases1 = searchClient.elasticClient
        .execute { catAliases() }
        .awaitFinite()
        .result

      aliases1.map(_.alias).to(List).sorted shouldBe List(
        "dataset",
        "file",
        "record"
      )

      searchClient
        .searchRecords()
        .awaitFinite()
        .records
        .length shouldBe 1 //one mocked record per dataset

      searchClient
        .searchDatasets(Some("test"))
        .awaitFinite()
        .datasets
        .length shouldBe 1

      searchClient
        .searchFiles(query = Some("brain.dcm"), None)
        .awaitFinite()
        .files
        .length shouldBe 1

      val dataset2 = createDatasetV1(searchPorts.db)(
        status = PublishStatus.PublishSucceeded,
        description = "bits, bytes, and beeps",
        sourceDatasetId = 2
      )
      createFile(searchPorts.db)(
        dataset2,
        path = "files/in/a/directory/brain.dcm",
        fileType = "DICOM"
      )

      // rebuilding should create another index

      Search
        .buildSearchIndex(searchPorts)
        .awaitFinite()

      val aliases2 = searchClient.elasticClient
        .execute { catAliases() }
        .awaitFinite()
        .result

      aliases2.map(_.alias).to(List).sorted shouldBe List(
        "dataset",
        "file",
        "record"
      )
      aliases2
        .map(_.index)
        .to(List)
        .sorted !== (aliases1.map(_.index).to(List).sorted)

      searchClient
        .searchDatasets(Some("bytes"))
        .awaitFinite()
        .datasets
        .length shouldBe 1

      searchClient
        .searchFiles(query = Some("brain.dcm"), None)
        .awaitFinite()
        .files
        .length shouldBe 2

      searchClient
        .searchDatasets()
        .awaitFinite()
        .datasets
        .length shouldBe 2

      searchClient
        .searchRecords()
        .awaitFinite()
        .records
        .length shouldBe 2 //one mocked record per dataset
    }

    "update the dataset document and add files when revising a dataset" in {
      val dataset =
        createDataset(searchPorts.db)()
      val version = createNewDatasetVersion(searchPorts.db)(
        dataset.id,
        status = PublishStatus.PublishSucceeded
      )
      val file = createFile(searchPorts.db)(
        version,
        path = "files/in/a/directory/brain.dcm",
        fileType = "DICOM"
      )

      val collection = TestUtilities.createCollection(ports.db)(
        datasetId = dataset.id,
        version = version.version,
        sourceCollectionId = 1
      )

      // Initial indexing
      Search
        .buildSearchIndex(searchPorts)
        .awaitFinite()

      val revision = Revision(dataset.id, version.version, revision = 1)

      val revisedReadme =
        FileManifest("revisions/1/readme.md", 12345L, FileType.Markdown, None)

      // Reindex while revising
      searchClient
        .indexRevision(
          dataset,
          version.copy(description = "bits, bytes, and beeps"),
          contributors = List.empty,
          revision = revision,
          files = List(revisedReadme),
          readme = Readme("A new readme that discusses flowers"),
          collections = List(collection),
          externalPublications = List.empty
        )
        .awaitFinite()

      // Should update dataset metadata
      searchClient
        .searchDatasets(Some("bytes"))
        .awaitFinite()
        .datasets
        .length shouldBe 1

      // And readme
      searchClient
        .searchDatasets(Some("flowers"))
        .awaitFinite()
        .datasets
        .length shouldBe 1

      // Index should still have old files for the dataset
      searchClient
        .searchFiles(query = Some("brain.dcm"), None)
        .awaitFinite()
        .files
        .length shouldBe 1

      // And also contain new files created by the revision
      searchClient
        .searchFiles(query = Some("readme.md"), None)
        .awaitFinite()
        .files
        .length shouldBe 1
    }

    "not index files and records for embargoed datasets" in {
      val dataset =
        createDataset(searchPorts.db)()
      val version = createNewDatasetVersion(searchPorts.db)(
        dataset.id,
        status = PublishStatus.EmbargoSucceeded
      )
      val file = createFile(searchPorts.db)(
        version,
        path = "files/in/a/directory/brain.dcm",
        fileType = "DICOM"
      )

      // Initial indexing
      Search
        .buildSearchIndex(searchPorts)
        .awaitFinite(10.seconds)

      // Should index dataset
      searchClient
        .searchDatasets()
        .awaitFinite()
        .datasets
        .length shouldBe 1

      // But not files
      searchClient
        .searchFiles()
        .awaitFinite()
        .files shouldBe empty

      // Nor records
      searchClient
        .searchRecords()
        .awaitFinite()
        .records shouldBe empty
    }

    val file1 =
      FileDocument(
        FileManifest("files/about.zip", 1000L, FileType.ZIP, None),
        dataset1
      )
    val file2 = FileDocument(
      FileManifest("files/brain.dcm", 12345L, FileType.DICOM, None),
      dataset2
    )
    val file3 = FileDocument(
      FileManifest("files/results.zip", 50000L, FileType.ZIP, None),
      dataset2
    )
    val file4 = FileDocument(
      FileManifest("files/zippy-the-dog.jpg", 824555L, FileType.JPEG, None),
      dataset1
    )
    val files = Seq(file1, file2, file3, file4)

    "construct a file uri" in {
      file1.uri shouldBe s"s3://discover/${dataset1.dataset.id}/${dataset1.dataset.version}/files/about.zip"
      file2.uri shouldBe s"s3://discover/${dataset2.dataset.id}/${dataset2.dataset.version}/files/brain.dcm"
    }

    "search files by name" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(query = Some("result"))
        .awaitFinite()
        .files
        .to(Set) shouldEqual Set(file3)
    }

    "handle file typos" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(query = Some("zipyp"))
        .awaitFinite()
        .files
        .to(Set) shouldBe Set(file4)
    }

    "search files by file type" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(fileType = Some("zip"))
        .awaitFinite()
        .files
        .to(Set) shouldEqual Set(file1, file3)
    }

    "search files by file type fuzzily" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(fileType = Some("jpg"))
        .awaitFinite()
        .files
        .to(Set) shouldEqual Set(file4)
    }

    "search files by organization" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(organization = Some("SPARC"))
        .awaitFinite()
        .files
        .to(Set) shouldBe Set(file2, file3)
    }

    "search files by organization id" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(organizationId = Some(10))
        .awaitFinite()
        .files
        .to(Set) shouldBe Set(file2, file3)
    }

    "search files by dataset" in {
      searchClient.insertFiles(files).awaitFinite()

      searchClient
        .searchFiles(datasetId = Some(dataset1.dataset.id))
        .awaitFinite()
        .files
        .to(Set) shouldBe Set(file1, file4)
    }

    "replace existing file entries when re-indexing a dataset" in {
      searchClient.insertFiles(Seq(file1)).awaitFinite()

      searchClient
        .searchFiles(fileType = Some("zip"))
        .awaitFinite()
        .files
        .length shouldBe 1

      val newVersion = dataset1.dataset.version + 1
      val newFile =
        file1.copy(dataset = dataset1.dataset.copy(version = newVersion))

      // TODO: call deleteFiles within insertFiles
      searchClient
        .deleteFiles(dataset1.dataset.id, Some(newVersion))
        .awaitFinite()
      searchClient.insertFiles(Seq(newFile)).awaitFinite()

      searchClient
        .searchFiles(fileType = Some("zip"))
        .awaitFinite()
        .files shouldBe Seq(newFile)
    }

    "handle limit and offset for files" in {
      searchClient.insertFiles(files).awaitFinite()

      val result1 = searchClient
        .searchFiles(limit = 2, offset = 0)
        .awaitFinite()
        .files

      result1.length shouldBe 2

      val result2 = searchClient
        .searchFiles(limit = 1, offset = 2)
        .awaitFinite()
        .files

      result2.length shouldBe 1

      Set(result1) should not contain result2
    }

    "index 100,000 files with a large README without OOM errors" in {
      val dataset =
        dataset1.copy(readme = Readme(TestUtilities.randomString(2000)))

      searchClient
        .insertFiles(
          List
            .range(0, 100000)
            .map(
              i =>
                FileDocument(
                  FileManifest(s"files/file$i.zip", 50000L, FileType.ZIP, None),
                  dataset
                )
            )
        )
        .awaitFinite(60.seconds)
    }

    val record1 =
      Record("patient", 1, 4, "SPARC", Map("name" -> "Joe", "age" -> "10"))
    val record2 =
      Record("medicine", 3, 5, "Blackfynn", Map("name" -> "aspirin"))

    val records = List(record1, record2)

    "search records" in {
      searchClient
        .insertRecords(records.map(RecordDocument(_)))
        .awaitFinite()

      searchClient
        .searchRecords()
        .awaitFinite()
        .records
        .map(_.record)
        .to(Set) shouldBe records.to(Set)
    }

    "search records by organization" in {
      searchClient
        .insertRecords(records.map(RecordDocument(_)))
        .awaitFinite()

      searchClient
        .searchRecords(organization = Some("Blackfynn"))
        .awaitFinite()
        .records
        .map(_.record) shouldBe List(record2)
    }

    "search records by dataset" in {
      searchClient
        .insertRecords(records.map(RecordDocument(_)))
        .awaitFinite()

      searchClient
        .searchRecords(datasetId = Some(record1.datasetId))
        .awaitFinite()
        .records
        .map(_.record) shouldBe List(record1)
    }

    "search records by model name" in {
      searchClient
        .insertRecords(records.map(RecordDocument(_)))
        .awaitFinite()

      searchClient
        .searchRecords(model = Some("medicine"))
        .awaitFinite()
        .records
        .map(_.record) shouldBe List(record2)
    }

    "search records by model name using a non-fuzzy query" in {
      val record1 =
        Record("Trial-balloon", 1, 4, "SPARC", Map())
      val record2 =
        Record("Trial-feeding", 1, 4, "SPARC", Map())

      searchClient
        .insertRecords(List(record1, record2).map(RecordDocument(_)))
        .awaitFinite()

      searchClient
        .searchRecords(model = Some("Trial-balloon"))
        .awaitFinite()
        .records
        .map(_.record) shouldBe List(record1)
    }
  }
}
