// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._
import com.pennsieve.discover.Config
import com.pennsieve.discover.server.definitions.PublicContributorDTO
import com.pennsieve.discover.models._
import com.pennsieve.models.FileManifest
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{
  ElasticClient,
  ElasticProperties,
  Response
}
import com.sksamuel.elastic4s.{ Index, IndexAndType }
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.analyzers._
import com.sksamuel.elastic4s.http.{ Functor, Handler }
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.delete.DeleteByQueryResponse
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.index.admin.AliasActionResponse
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.sksamuel.elastic4s.mappings.FieldType._
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.searches.sort.SortOrder
import com.sksamuel.elastic4s.searches.queries.{
  Query,
  SimpleQueryStringFlag,
  SimpleStringQuery
}
import com.typesafe.scalalogging.StrictLogging

import java.time.OffsetDateTime
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.higherKinds

trait SearchClient {

  /**
    * Main aliases that we query against.
    * These are backed by another index which is aliased to this name for
    * quick cutover.
    *
    * See https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
    */
  val datasetAlias: Index = Index("dataset")
  val fileAlias: Index = Index("file")
  val recordAlias: Index = Index("record")

  /**
    * Elastic4s requires a 'type` along with an index when inserting documents.
    * This is a deprecated feature in Elastic, so the type is not used anywhere else.
    */
  val defaultType: String = "document"
  def indexAndType(index: Index): IndexAndType = index.name / defaultType

  /**
    * Index mapping for datasets
    */
  val datasetMapping: MappingDefinition =
    MappingDefinition(defaultType) as (
      textField("dataset.name").fields(
        textField("english").analyzer(EnglishLanguageAnalyzer)
      ),
      textField("dataset.description").fields(
        textField("english").analyzer(EnglishLanguageAnalyzer)
      ),
      booleanField("dataset.embargo").nullable(true),
      textField("dataset.tags").fields(
        textField("english").analyzer(EnglishLanguageAnalyzer),
        keywordField("raw")
      ),
      textField("readme").fields(
        textField("english").analyzer(EnglishLanguageAnalyzer)
      ),
      textField("contributors").fields(
        textField("english").analyzer(EnglishLanguageAnalyzer),
        keywordField("raw")
      ),
      dateField("dataset.createdAt"),
      // Don't index these metadata fields
      objectField("dataset.readme").enabled(false),
      objectField("dataset.contributors").enabled(false),
      objectField("dataset.banner").enabled(false),
      objectField("dataset.updatedAt").enabled(false),
      objectField("dataset.uri").enabled(false),
      objectField("dataset.arn").enabled(false),
      objectField("dataset.revision").enabled(false),
      objectField("dataset.revisedAt").enabled(false),
      // This is a hack to get ordering by dataset name to work. ElasticSearch
      // lets you set "normalizers" on keyword fields that automatically perform
      // transformations at index and query time, by Elastic4s is failing with a
      // null pointer exception when trying to add a normalizer to the index.
      //
      // As a workaround, we add this normalized keyword field to
      // `DatasetDocument`. This works ok since we only need to sort be this
      // field, and not query it.
      //
      // See https://www.elastic.co/guide/en/elasticsearch/reference/current/normalizer.html
      keywordField("rawName"),
  )

  /**
    * Index mapping for files
    */
  val fileMapping: MappingDefinition =
    MappingDefinition(defaultType) as (
      textField("name")
        .fields(
          textField("english").analyzer(EnglishLanguageAnalyzer),
          // Split field on non-letter characters so that "results.zip"
          // can be found by querying "result".
          textField("tokens").analyzer(SimpleAnalyzer)
        )
      )

  val recordMapping: MappingDefinition =
    MappingDefinition(defaultType) as (
      textField("record.model").fields(keywordField("raw")),
      // Don't index these metadata fields
      objectField("record.properties").enabled(false)
  )

  def indexDataset(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    readme: Readme,
    revision: Option[Revision],
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    sponsorship: Option[Sponsorship],
    files: Source[PublicFile, NotUsed],
    records: Source[Record, NotUsed],
    datasetIndex: Option[Index] = None,
    fileIndex: Option[Index] = None,
    recordIndex: Option[Index] = None
  )(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): Future[Done]

  def indexSponsoredDataset(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    readme: Readme,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    revision: Option[Revision] = None,
    sponsorship: Option[Sponsorship] = None,
    index: Option[Index] = None
  )(implicit
    config: Config
  ): Future[Done]

  def removeDataset(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): Future[Done]

  def deleteDataset(datasetId: Int): Future[Done]

  def indexRevision(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    revision: Revision,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    files: List[FileManifest],
    readme: Readme,
    sponsorship: Option[Sponsorship] = None
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Future[Done]

  /**
    * Search the contents of the dataset index.
    */
  def searchDatasets(
    query: Option[String] = None,
    organization: Option[String] = None,
    organizationId: Option[Int] = None,
    tags: Option[List[String]] = None,
    embargo: Option[Boolean] = None,
    limit: Int = 10,
    offset: Int = 0,
    orderBy: OrderBy = OrderBy.default,
    orderDirection: OrderDirection = OrderDirection.default
  ): Future[DatasetSearchResponse]

  /**
    * Search the contents of the file index.
    */
  def searchFiles(
    query: Option[String] = None,
    fileType: Option[String] = None,
    organization: Option[String] = None,
    organizationId: Option[Int] = None,
    datasetId: Option[Int] = None,
    limit: Int = 10,
    offset: Int = 0
  ): Future[FileSearchResponse]

  /**
    * Search individual metadata records.
    */
  def searchRecords(
    organization: Option[String] = None,
    datasetId: Option[Int] = None,
    model: Option[String] = None,
    limit: Int = 10,
    offset: Int = 0
  ): Future[RecordSearchResponse]

  /**
    * Build a index from scratch in the background. When all elements are
    * inserted, the main index alias is cut over to use this old index.
    *
    * The `buildIndex` callback is responsible for inserting the
    * appropriate items into the new index.
    *
    * TODO: make this monadic to simplify usage in `Search.buildSearchIndex`
    */
  def buildIndexAndReplaceAlias[T](
    index: Index,
    mapping: MappingDefinition
  )(
    builder: Index => Future[T]
  ): Future[T]
}

case class FileSearchResponse(
  totalCount: Long,
  files: Seq[FileDocument],
  limit: Int,
  offset: Int
)

case class DatasetSearchResponse(
  totalCount: Long,
  datasets: Seq[DatasetDocument],
  limit: Int,
  offset: Int
)

case class RecordSearchResponse(
  totalCount: Long,
  records: Seq[RecordDocument],
  limit: Int,
  offset: Int
)

class AwsElasticSearchClient(
  elasticUri: String,
  config: Config,
  refreshPolicy: RefreshPolicy = RefreshPolicy.None
)(implicit
  system: ActorSystem,
  executionContext: ExecutionContext
) extends SearchClient
    with StrictLogging {

  val elasticClient: ElasticClient = ElasticClient(
    ElasticProperties(elasticUri)
  )

  /**
    * Wrapper around Elastic client that always maps the result in order to
    * propagate errors.
    */
  private def execute[T, U](
    t: T
  )(implicit
    handler: Handler[T, U],
    manifest: Manifest[U],
    executionContext: ExecutionContext
  ): Future[U] =
    elasticClient
      .execute(t)
      .map(_.result)

  def indexDataset(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    readme: Readme,
    revision: Option[Revision],
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    sponsorship: Option[Sponsorship],
    files: Source[PublicFile, NotUsed],
    records: Source[Record, NotUsed],
    datasetIndex: Option[Index] = None,
    fileIndex: Option[Index] = None,
    recordIndex: Option[Index] = None
  )(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
  ): Future[Done] = {

    val datasetDocument = DatasetDocument(
      dataset,
      version,
      contributors,
      readme,
      revision,
      sponsorship,
      collections,
      externalPublications
    )(config)

    for {
      _ <- insertDataset(datasetDocument, datasetIndex)

      _ <- insertFileStream(
        files.map(FileDocument(_, datasetDocument)),
        fileIndex
      )
      _ <- insertRecordStream(records, recordIndex)

      // Remove files and records from previously published versions
      _ <- deleteFiles(dataset.id, Some(version.version), fileIndex)
      _ <- deleteRecords(dataset.id, Some(version.version), recordIndex)
    } yield Done
  }

  def indexRevision(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    revision: Revision,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    files: List[FileManifest],
    readme: Readme,
    sponsorship: Option[Sponsorship] = None
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Future[Done] = {

    val datasetDocument =
      DatasetDocument(
        dataset,
        version,
        contributors,
        readme,
        Some(revision),
        sponsorship,
        collections,
        externalPublications
      )(config)
    for {
      _ <- insertDataset(datasetDocument)
      _ <- insertFiles(files.map(FileDocument(_, datasetDocument)))
    } yield Done
  }

  def removeDataset(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): Future[Done] =
    for {
      _ <- deleteDataset(dataset.id)
      _ <- deleteFiles(dataset.id, None)
      _ <- deleteRecords(dataset.id, None)
    } yield Done

  def indexSponsoredDataset(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    readme: Readme,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    revision: Option[Revision] = None,
    sponsorship: Option[Sponsorship] = None,
    index: Option[Index] = None
  )(implicit
    config: Config
  ): Future[Done] = insertDataset(
    DatasetDocument(
      dataset,
      version,
      contributors,
      readme,
      revision,
      sponsorship,
      collections,
      externalPublications
    ),
    index
  )

  def insertDataset(
    dataset: DatasetDocument,
    index: Option[Index] = None
  ): Future[Done] = insertDatasets(Seq(dataset), index = index)

  def insertDatasets(
    datasets: Seq[DatasetDocument],
    index: Option[Index] = None
  ): Future[Done] =
    execute(
      bulk(
        datasets.map(
          dataset =>
            indexInto(indexAndType(index.getOrElse(datasetAlias)))
              .doc(dataset)
              .id(dataset.id)
        ): _*
      ).refresh(refreshPolicy)
    ).map(_ => Done)

  /**
    * Insert files into the file index.
    *
    * Since files do not have a natural key to use for indexing, Elastic
    * generates a unique id.
    *
    * Inserts are batched to prevent OOM errors when ingesting large datasets.
    */
  def insertFiles(
    files: Seq[FileDocument],
    index: Option[Index] = None
  ): Future[Done] = {
    if (files.nonEmpty)
      Future
        .traverse(
          files
            .map(
              file =>
                indexInto(indexAndType(index.getOrElse(fileAlias)))
                  .doc(file)
            )
            .grouped(1000)
        )(statements => execute(bulk(statements: _*).refresh(refreshPolicy)))
        .map(_ => Done)
    else
      Future.successful(Done)
  }

  def insertFileStream(
    files: Source[FileDocument, NotUsed],
    index: Option[Index] = None
  ): Future[Done] =
    files.grouped(1000).mapAsync(1)(insertFiles(_, index)).runWith(Sink.ignore)

  /**
    * Insert metadata records into ElasticSearch.
    */
  def insertRecords(
    records: Seq[RecordDocument],
    index: Option[Index] = None
  ): Future[Done] =
    if (records.nonEmpty)
      execute(
        bulk(
          records
            .map(
              record =>
                indexInto(indexAndType(index.getOrElse(recordAlias)))
                  .doc(record)
            )
        ).refresh(refreshPolicy)
      ).map(_ => Done)
    else
      Future.successful(Done)

  def insertRecordStream(
    records: Source[Record, NotUsed],
    index: Option[Index] = None
  ): Future[Done] =
    records
      .map(RecordDocument(_))
      .grouped(1000)
      .mapAsync(1)(insertRecords(_, index))
      .runWith(Sink.ignore)

  /**
    * Delete all documents for this dataset from the index.
    */
  def deleteDataset(datasetId: Int): Future[Done] =
    execute(
      deleteByQuery(
        datasetAlias,
        defaultType,
        termQuery("dataset.id", datasetId)
      )
    ).map(_ => Done)

  /**
    * Delete all file entries for this dataset from the index, *excluding* those
    * for the given version.
    */
  def deleteFiles(
    datasetId: Int,
    keepVersion: Option[Int],
    index: Option[Index] = None
  ): Future[Done] = {

    val query = keepVersion match {
      case Some(version) =>
        boolQuery()
          .must(termQuery("dataset.id", datasetId))
          .not(termQuery("dataset.version", version))
      case None =>
        boolQuery()
          .must(termQuery("dataset.id", datasetId))
    }

    execute(deleteByQuery(index.getOrElse(fileAlias), defaultType, query))
      .map(_ => Done)
  }

  /**
    * Delete all records for this dataset from the index, *excluding* those
    * for the given version.
    */
  def deleteRecords(
    datasetId: Int,
    keepVersion: Option[Int],
    index: Option[Index] = None
  ): Future[DeleteByQueryResponse] = {

    val query = keepVersion match {
      case Some(version) =>
        boolQuery()
          .must(termQuery("record.datasetId", datasetId))
          .not(termQuery("record.version", version))
      case None =>
        boolQuery()
          .must(termQuery("record.datasetId", datasetId))
    }

    execute(deleteByQuery(index.getOrElse(recordAlias), defaultType, query))
  }

  /**
    * Don't perform fuzzy searches on strings shorter than this.
    */
  val prefixLength: Int = 2
  val maxExpansions: Int = 20
  val fuzziness: String = "AUTO"

  /**
    * Search over datasets using a `simple_string_query`:
    * https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html#simple-query-string-syntax
    */
  def searchDatasets(
    query: Option[String] = None,
    organization: Option[String] = None,
    organizationId: Option[Int] = None,
    tags: Option[List[String]] = None,
    embargo: Option[Boolean] = None,
    limit: Int = 10,
    offset: Int = 0,
    orderBy: OrderBy = OrderBy.default,
    orderDirection: OrderDirection = OrderDirection.default
  ): Future[DatasetSearchResponse] = {

    // TODO: use `quote_field_suffix` for quoted phrases?
    val elasticQuery: Option[Query] =
      query
        .map(
          simpleStringQuery(_)
            .flags(SimpleQueryStringFlag.ALL)
            .defaultOperator("AND")
            .analyzer(EnglishLanguageAnalyzer)
        )

    val filters = List(
      organization
        .map(matchQuery("dataset.organizationName", _))
        .toList,
      organizationId.map(termQuery("dataset.organizationId", _)).toList,
      embargo.toList.map(termQuery("dataset.embargo", _)),
      tags.toList.flatten
        .map(_.toLowerCase)
        .map(termQuery("dataset.tags.raw", _))
    ).flatten

    val direction: SortOrder = orderDirection match {
      case OrderDirection.Ascending => SortOrder.Asc
      case OrderDirection.Descending => SortOrder.Desc
    }

    val sort = orderBy match {
      case OrderBy.Relevance => scoreSort().order(direction)
      case OrderBy.Date => fieldSort("dataset.createdAt").order(direction)
      case OrderBy.Name => fieldSort("rawName").order(direction)
      case OrderBy.Size => fieldSort("dataset.size").order(direction)
    }

    execute(
      search(datasetAlias)
        .query(boolQuery().must(elasticQuery.toList).filter(filters))
        .sortBy(sort)
        .start(offset)
        .limit(limit)
    ).map(
      result =>
        DatasetSearchResponse(
          totalCount = result.hits.total,
          datasets = result.to[DatasetDocument],
          limit = limit,
          offset = offset
        )
    )
  }

  def searchFiles(
    query: Option[String] = None,
    fileType: Option[String] = None,
    organization: Option[String] = None,
    organizationId: Option[Int] = None,
    datasetId: Option[Int] = None,
    limit: Int = 10,
    offset: Int = 0
  ): Future[FileSearchResponse] = {

    val queries = List(
      query.map(
        // Filename is the most important field when searching files.
        multiMatchQuery(_)
          .fields("name.*")
          .fuzziness(fuzziness)
          .prefixLength(prefixLength)
          .maxExpansions(maxExpansions)
          .boost(2)
      ),
      // However, we still want to match files based on other fields
      query.map(
        multiMatchQuery(_)
          .fuzziness(fuzziness)
          .prefixLength(prefixLength)
          .maxExpansions(maxExpansions)
          .boost(0.5)
      ),
      // File type should be slightly fuzzy, eg so that JPG matches JPEG
      fileType.map(
        matchQuery("file.fileType", _)
          .fuzziness(fuzziness)
          .prefixLength(prefixLength)
          .maxExpansions(maxExpansions)
      ),
      organization.map(matchQuery("dataset.organizationName", _)),
      organizationId.map(termQuery("dataset.organizationId", _)),
      datasetId.map(termQuery("dataset.id", _))
    ).collect {
      case Some(query) => query
    }

    execute(
      search(fileAlias)
        .query(boolQuery().must(queries))
        .start(offset)
        .limit(limit)
    ).map(
      result =>
        FileSearchResponse(
          totalCount = result.hits.total,
          files = result.to[FileDocument],
          limit = limit,
          offset = offset
        )
    )
  }

  def searchRecords(
    organization: Option[String] = None,
    datasetId: Option[Int] = None,
    model: Option[String] = None,
    limit: Int = 100,
    offset: Int = 0
  ): Future[RecordSearchResponse] = {
    val queries = List(
      organization.map(matchQuery("record.organizationName", _)),
      datasetId.map(termQuery("record.datasetId", _)),
      model.map(termQuery("record.model.raw", _))
    ).collect {
      case Some(query) => query
    }

    execute(
      search(recordAlias)
        .query(boolQuery().must(queries))
        .start(offset)
        .limit(limit)
    ).map(
      result =>
        RecordSearchResponse(
          totalCount = result.hits.total,
          records = result.to[RecordDocument],
          limit = limit,
          offset = offset
        )
    )
  }

  def buildIndexAndReplaceAlias[T](
    alias: Index,
    mapping: MappingDefinition
  )(
    buildIndex: Index => Future[T]
  ): Future[T] = {
    val newIndex = Index(
      s"${alias.name}-${OffsetDateTime.now().toInstant().toEpochMilli()}"
    )

    logger.info(s"Creating new index ${newIndex.name}")

    for {
      // Find existing indices that are using this alias
      aliasedIndices <- execute(catAliases())
        .map(_.filter(_.alias == alias.name))

      _ <- execute(
        createIndex(newIndex.name)
        // in Elasticsearch 6.x the default number of shards is 5, but in 7.x this will change to 1.
        // 6.x issues a warning if this is not set explicitly.
        // It also complains if "include_type_name" is not set explicitly, but elastic4s does not
        // provide a way to set this query parameter.
          .shards(5)
          .mappings(mapping)
      )

      _ <- execute(refreshIndex(newIndex.name))
      result <- buildIndex(newIndex)

      removeAliases = aliasedIndices
        .map(alias => removeAlias(alias.alias).on(alias.index))

      _ <- execute(
        aliases(addAlias(alias.name).on(newIndex.name), removeAliases: _*)
      )

      // Now that the new index has been promoted, it's safe to delete the old index
      _ <- if (!aliasedIndices.isEmpty) {
        aliasedIndices.foreach(
          alias => logger.info(s"Deleting index ${alias.index}")
        )
        execute(deleteIndex(aliasedIndices.map(_.index): _*))
      } else Future.successful(())

    } yield result
  }
}
