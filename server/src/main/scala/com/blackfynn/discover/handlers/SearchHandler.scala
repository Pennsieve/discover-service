// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.pennsieve.discover.{ BadQueryParameter, Config, Ports }
import com.pennsieve.discover.db.PublicDatasetVersionsMapper
import com.pennsieve.discover.models.{
  DatasetDocument,
  DatasetsPage,
  FileDocument,
  FilesPage,
  OrderBy,
  OrderDirection,
  RecordPage
}
import com.pennsieve.discover.logging.logRequestAndResponse
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.server.search.{
  SearchHandler => GuardrailHandler,
  SearchResource => GuardrailResource
}
import com.pennsieve.models.PublishStatus
import com.sksamuel.elastic4s.circe._

import scala.concurrent.{ ExecutionContext, Future }

class SearchHandler(
  ports: Ports
)(implicit
  executionContext: ExecutionContext,
  materializer: ActorMaterializer
) extends GuardrailHandler {

  implicit val config: Config = ports.config

  val defaultLimit = 10
  val defaultOffset = 0

  /**
    * Search for datasets in ElasticSearch
    */
  override def searchDatasets(
    respond: GuardrailResource.searchDatasetsResponse.type
  )(
    limit: Option[Int],
    offset: Option[Int],
    query: Option[String],
    organization: Option[String],
    organizationId: Option[Int],
    tags: Option[Iterable[String]],
    embargo: Option[Boolean],
    orderBy: Option[String],
    orderDirection: Option[String]
  ): Future[GuardrailResource.searchDatasetsResponse] = {

    val actualLimit = limit.getOrElse(defaultLimit)
    val actualOffset = offset.getOrElse(defaultOffset)

    val finalQuery = query match {
      case Some(query) if query.trim.isEmpty => None
      case query => query
    }

    val response = for {
      orderBy <- param.parse(
        orderBy,
        OrderBy.withNameInsensitive,
        OrderBy.default
      )

      orderDirection <- param.parse(
        orderDirection,
        OrderDirection.withNameInsensitive,
        OrderDirection.default
      )

      datasetPage <- ports.searchClient
        .searchDatasets(
          query = finalQuery,
          organization = organization,
          organizationId = organizationId,
          tags = tags.map(_.toList),
          embargo = embargo,
          limit = actualLimit,
          offset = actualOffset,
          orderBy = orderBy,
          orderDirection = orderDirection
        )
        .map(
          DatasetsPage
            .apply(_)
        )

    } yield
      GuardrailResource.searchDatasetsResponse
        .OK(datasetPage)

    response.recover {
      case e @ BadQueryParameter(_) =>
        GuardrailResource.searchDatasetsResponse
          .BadRequest(e.getMessage)
    }
  }

  override def searchFiles(
    respond: GuardrailResource.searchFilesResponse.type
  )(
    limit: Option[Int],
    offset: Option[Int],
    fileType: Option[String],
    query: Option[String],
    organization: Option[String],
    organizationId: Option[Int],
    datasetId: Option[Int]
  ): Future[GuardrailResource.searchFilesResponse] =
    for {
      searchResult <- ports.searchClient
        .searchFiles(
          query = query,
          fileType = fileType,
          organization = organization,
          organizationId = organizationId,
          datasetId = datasetId,
          limit = limit.getOrElse(defaultLimit),
          offset = offset.getOrElse(defaultOffset)
        )
    } yield
      GuardrailResource.searchFilesResponse
        .OK(
          FilesPage
            .apply(searchResult)
        )

  override def searchRecords(
    respond: GuardrailResource.searchRecordsResponse.type
  )(
    limit: Option[Int],
    offset: Option[Int],
    model: Option[String],
    organization: Option[String],
    datasetId: Option[Int]
  ): Future[GuardrailResource.searchRecordsResponse] =
    for {
      searchResult <- ports.searchClient
        .searchRecords(
          organization = organization,
          datasetId = datasetId,
          model = model,
          limit = limit.getOrElse(defaultLimit),
          offset = offset.getOrElse(defaultOffset)
        )
    } yield
      GuardrailResource.searchRecordsResponse
        .OK(
          RecordPage
            .apply(searchResult)
        )
}

object SearchHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      GuardrailResource.routes(new SearchHandler(ports))
    }
}
