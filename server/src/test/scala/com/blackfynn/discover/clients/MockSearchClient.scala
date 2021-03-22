// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._
import com.blackfynn.discover.Config
import com.blackfynn.discover.models._
import com.blackfynn.models.FileManifest
import com.blackfynn.test.AwaitableImplicits
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.{
  ElasticClient,
  ElasticProperties,
  Response
}
import com.sksamuel.elastic4s.{ Index, IndexAndType }
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.delete.DeleteByQueryResponse
import com.sksamuel.elastic4s.http.index.IndexResponse
import com.sksamuel.elastic4s.http.index.admin.AliasActionResponse
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.sksamuel.elastic4s.RefreshPolicy
import com.typesafe.scalalogging.StrictLogging
import java.time.OffsetDateTime

import scala.collection.mutable
import scala.concurrent.{ ExecutionContext, Future }

class MockSearchClient extends SearchClient with AwaitableImplicits {

  val indexedDatasets: mutable.Map[
    Int,
    (
      PublicDatasetVersion,
      Option[Revision],
      Option[Sponsorship],
      List[PublicFile],
      List[Record]
    )
  ] =
    mutable.Map
      .empty[
        Int,
        (
          PublicDatasetVersion,
          Option[Revision],
          Option[Sponsorship],
          List[PublicFile],
          List[Record]
        )
      ]

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
    materializer: ActorMaterializer
  ): Future[Done] = {

    val indexedFiles = files.runWith(Sink.seq).map(_.toList).awaitFinite()
    val indexedRecords = records.runWith(Sink.seq).map(_.toList).awaitFinite()

    indexedDatasets += (
      (
        dataset.id,
        (version, revision, sponsorship, indexedFiles, indexedRecords)
      )
    )
    Future.successful(Done)
  }

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
  ): Future[Done] = {
    indexedDatasets += (
      (
        dataset.id,
        (version, revision, sponsorship, List.empty, List.empty)
      )
    )
    Future.successful(Done)
  }

  def removeDataset(
    dataset: PublicDataset
  )(implicit
    executionContext: ExecutionContext
  ): Future[Done] = deleteDataset(dataset.id)

  def deleteDataset(datasetId: Int): Future[Done] = {
    indexedDatasets -= datasetId
    Future.successful(Done)
  }

  def indexRevision(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    revision: Revision,
    collections: List[PublicCollection] = List.empty,
    externalPublications: List[PublicExternalPublication],
    files: List[FileManifest],
    readme: Readme,
    sponsorship: Option[Sponsorship] = None
  )(implicit
    executionContext: ExecutionContext,
    materializer: ActorMaterializer
  ): Future[Done] = {
    indexedDatasets += (
      (
        dataset.id,
        (version, Some(revision), sponsorship, List.empty, List.empty)
      )
    )
    Future.successful(Done)
  }

  val datasetSearchResults: mutable.ArrayBuffer[DatasetDocument] =
    mutable.ArrayBuffer.empty

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
    Future.successful(
      DatasetSearchResponse(
        totalCount = datasetSearchResults.length,
        datasets = datasetSearchResults,
        limit = limit,
        offset = offset
      )
    )
  }

  val recordSearchResults: mutable.ArrayBuffer[RecordDocument] =
    mutable.ArrayBuffer.empty

  def searchRecords(
    organization: Option[String] = None,
    datasetId: Option[Int] = None,
    model: Option[String] = None,
    limit: Int = 10,
    offset: Int = 0
  ): Future[RecordSearchResponse] = Future.successful(
    RecordSearchResponse(
      totalCount = recordSearchResults.length,
      records = recordSearchResults,
      limit = limit,
      offset = offset
    )
  )

  val fileSearchResults: mutable.ArrayBuffer[FileDocument] =
    mutable.ArrayBuffer.empty

  def searchFiles(
    query: Option[String] = None,
    fileType: Option[String] = None,
    organization: Option[String] = None,
    organizationId: Option[Int] = None,
    datasetId: Option[Int] = None,
    limit: Int = 10,
    offset: Int = 0
  ): Future[FileSearchResponse] =
    Future.successful(
      FileSearchResponse(
        totalCount = fileSearchResults.length,
        files = fileSearchResults,
        limit = limit,
        offset = offset
      )
    )

  def buildIndexAndReplaceAlias[T](
    index: Index,
    mapping: MappingDefinition
  )(
    builder: Index => Future[T]
  ): Future[T] = ???

  def clear(): Unit = {
    indexedDatasets.clear()
    datasetSearchResults.clear()
    fileSearchResults.clear()
    recordSearchResults.clear()
  }

}
