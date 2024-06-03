// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._
import com.pennsieve.discover.Config
import com.pennsieve.discover.models._
import com.pennsieve.models.FileManifest
import com.pennsieve.test.AwaitableImplicits
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.circe._
import com.sksamuel.elastic4s.requests.mappings.MappingDefinition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }

class MockSearchClient extends SearchClient with AwaitableImplicits {

  val indexedDatasets: mutable.Map[
    Int,
    (
      PublicDatasetVersion,
      Option[Revision],
      Option[Sponsorship],
      List[PublicFileVersion],
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
          List[PublicFileVersion],
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
    files: Source[PublicFileVersion, NotUsed],
    records: Source[Record, NotUsed],
    datasetIndex: Option[Index] = None,
    fileIndex: Option[Index] = None,
    recordIndex: Option[Index] = None,
    overwrite: Boolean = false
  )(implicit
    executionContext: ExecutionContext,
    system: ActorSystem
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
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Future[Done] = {
    indexedDatasets += (
      (
        dataset.id,
        (version, Some(revision), sponsorship, List.empty, List.empty)
      )
    )
    Future.successful(Done)
  }

  val datasetSearchResults: ArrayBuffer[DatasetDocument] =
    ArrayBuffer.empty

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
        datasets = datasetSearchResults.toSeq,
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
      records = recordSearchResults.toSeq,
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
        files = fileSearchResults.toSeq,
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
