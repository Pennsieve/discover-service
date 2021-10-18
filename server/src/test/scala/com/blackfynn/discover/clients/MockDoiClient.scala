// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.http.scaladsl.model.{ HttpHeader, HttpRequest, HttpResponse }
import akka.stream.Materializer
import cats.implicits._
import cats.data.EitherT
import com.pennsieve.discover.NoDoiException
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.definitions.InternalContributor
import com.pennsieve.doi.client.definitions._
import com.pennsieve.doi.client.doi.{
  CreateDraftDoiResponse,
  GetLatestDoiResponse,
  HideDoiResponse,
  PublishDoiResponse
}
import com.pennsieve.doi.models.DoiState.Registered
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.models.License
import io.circe.syntax._

import scala.collection.mutable.{ ArrayBuffer, Map }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Random

class MockDoiClient(
  httpClient: HttpRequest => Future[HttpResponse],
  ec: ExecutionContext,
  mat: Materializer
) extends DoiClient("https://mock-doi-service-host")(httpClient, ec, mat) {

  val dois: Map[String, DoiDTO] = Map.empty[String, DoiDTO]

  def createMockDoi(organizationId: Int, datasetId: Int): DoiDTO = {
    val doi = s"bfPrefix/${randomString()}"
    val dto = DoiDTO(
      organizationId = organizationId,
      datasetId = datasetId,
      doi = doi,
      publisher = "Pennsieve Discover",
      url = None,
      createdAt = Some("4/18/2019"),
      publicationYear = None,
      state = Some(DoiState.Draft)
    )
    dois += doi -> dto
    dto
  }

  def getMockDoi(organizationId: Int, datasetId: Int): Option[DoiDTO] = {
    dois
      .find {
        case (_, dto) =>
          dto.organizationId == organizationId && dto.datasetId == datasetId
      }
      .map(_._2)
  }

  override def createDraftDoi(
    organizationId: Int,
    datasetId: Int,
    headers: List[HttpHeader]
  ): Future[DoiDTO] =
    Future.successful(createMockDoi(organizationId, datasetId))

  override def getLatestDoi(
    organizationId: Int,
    datasetId: Int,
    headers: List[HttpHeader]
  ): Future[DoiDTO] =
    getMockDoi(organizationId, datasetId) match {
      case Some(doi) => Future.successful(doi)
      case None => Future.failed(NoDoiException)
    }

  override def publishDoi(
    doi: String,
    name: String,
    publicationYear: Int,
    contributors: List[PublicContributor],
    url: String,
    publisher: Option[String],
    owner: Option[InternalContributor] = None,
    version: Option[Int] = None,
    description: Option[String] = None,
    license: Option[License] = None,
    collections: List[PublicCollection] = List.empty,
    externalPublications: List[PublicExternalPublication] = List.empty,
    headers: List[HttpHeader]
  ): Future[DoiDTO] =
    dois.get(doi) match {
      case Some(dto) => {
        val published = dto.copy(
          title = Some(name),
          creators = Some(contributors.map(c => Some(c.fullName))),
          publicationYear = Some(publicationYear),
          url = Some(url)
        )
        dois += doi -> published
        Future.successful(published)
      }
      case None => Future.failed(NoDoiException)
    }

  override def reviseDoi(
    doi: String,
    name: String,
    contributors: List[PublicContributor],
    owner: Option[InternalContributor] = None,
    version: Option[Int] = None,
    description: Option[String] = None,
    license: Option[License] = None,
    collections: List[PublicCollection] = List.empty,
    externalPublications: List[PublicExternalPublication] = List.empty,
    headers: List[HttpHeader]
  ): Future[DoiDTO] =
    dois.get(doi) match {
      case Some(dto) => {
        val revised = dto.copy(
          title = Some(name),
          creators = Some(contributors.map(c => Some(c.fullName)))
        )
        dois += doi -> revised
        Future.successful(revised)
      }
      case None => Future.failed(NoDoiException)
    }

  override def hideDoi(doi: String, headers: List[HttpHeader]): Future[DoiDTO] =
    dois.get(doi) match {
      case Some(dto) => {
        val hidden = dto.copy(state = Some(Registered))
        dois += doi -> hidden
        Future.successful(hidden)
      }
      case None => Future.failed(NoDoiException)
    }

  def randomString(length: Int = 8): String =
    Random.alphanumeric take length mkString
}
