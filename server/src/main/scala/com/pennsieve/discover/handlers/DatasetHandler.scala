// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers
import java.time.LocalDate

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{
  `Content-Disposition`,
  Authorization,
  ContentDispositionTypes,
  OAuth2BearerToken
}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.data.EitherT
import cats.implicits._
import com.pennsieve.auth.middleware.AkkaDirective.authenticateJwt
import com.pennsieve.auth.middleware.{ Jwt, UserClaim }
import com.pennsieve.discover._
import com.pennsieve.discover.clients.{
  APIPreviewAccessRequest,
  AuthorizationClient,
  DatasetPreview,
  HttpError
}
import com.pennsieve.discover.db._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.downloads.ZipStream
import com.pennsieve.discover.logging.{
  logRequestAndResponse,
  DiscoverLogContext
}
import com.pennsieve.discover.models._
import com.pennsieve.discover.server.dataset.{
  DatasetHandler => GuardrailHandler,
  DatasetResource => GuardrailResource
}
import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.server.definitions.{
  DownloadRequest,
  DownloadResponse,
  DownloadResponseHeader,
  FileTreePage,
  PreviewAccessRequest,
  PublicDatasetDTO
}
import com.pennsieve.models.PublishStatus
import com.pennsieve.models.PublishStatus.{
  EmbargoSucceeded,
  NotPublished,
  PublishSucceeded,
  Unpublished
}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Handler for public endpoints for getting/downloading published datasets.
  * Nginx rewrites /discover to /public and routes requests to this service.
  */
class DatasetHandler(
  ports: Ports,
  authorization: Option[Authorization],
  claim: Option[Jwt.Claim]
)(implicit
  executionContext: ExecutionContext,
  system: ActorSystem
) extends GuardrailHandler {

  implicit val config: Config = ports.config

  val defaultDatasetLimit = 10
  val defaultDatasetOffset = 0

  val defaultFileLimit = 100
  val defaultFileOffset = 0

  /**
    * Optional authorization for embargoed datasets
    *
    * Reach out to `authorization-service` to determine if the current user has
    * permission to access embargoed data.
    */
  def authorizeIfUnderEmbargo(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  ): DBIO[Unit] =
    (version.underEmbargo, authorization) match {
      case (true, None) => DBIO.failed(UnauthorizedException)
      case (true, Some(authorization)) =>
        DBIO.from {
          ports.authorizationClient
            .authorizeEmbargoPreview(
              organizationId = dataset.sourceOrganizationId,
              datasetId = dataset.sourceDatasetId,
              authorization = authorization
            )
            .foldF(
              Future.failed(_),
              _ match {
                case AuthorizationClient.EmbargoAuthorization.OK =>
                  Future.successful(())
                case AuthorizationClient.EmbargoAuthorization.Unauthorized =>
                  Future.failed(UnauthorizedException)
                case AuthorizationClient.EmbargoAuthorization.Forbidden =>
                  Future.failed(DatasetUnderEmbargo)
              }
            )
        }
      case _ => DBIO.successful(())
    }

  /**
    * Helper query all to construct a dataset DTO for the given
    * dataset and version.
    *
    * Only use this for endpoints that return a single dataset.
    */
  def publicDatasetDTO(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    preview: Option[DatasetPreview] = None
  ): DBIO[PublicDatasetDTO] =
    for {

      (contributors, collections, externalPublications, sponsorship, revision) <- PublicDatasetVersionsMapper
        .getDatasetDetails(dataset, version)

    } yield
      models.PublicDatasetDTO(
        dataset,
        version,
        contributors.toSeq,
        sponsorship,
        revision,
        collections.toSeq,
        externalPublications.toSeq,
        preview
      )

  override def getDatasets(
    respond: GuardrailResource.getDatasetsResponse.type
  )(
    limit: Option[Int],
    offset: Option[Int],
    ids: Option[Iterable[String]],
    tags: Option[Iterable[String]],
    embargo: Option[Boolean],
    orderBy: Option[String],
    orderDirection: Option[String]
  ): Future[GuardrailResource.getDatasetsResponse] = {

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

      intIds = ids.map(_.map(_.toInt)).map(_.toList)

      pagedResult <- ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = tags.map(_.toList),
            embargo = embargo,
            limit = limit.getOrElse(defaultDatasetLimit),
            offset = offset.getOrElse(defaultDatasetOffset),
            orderBy = orderBy,
            orderDirection = orderDirection,
            ids = intIds
          )
        )
    } yield
      GuardrailResource.getDatasetsResponse
        .OK(DatasetsPage.apply(pagedResult))

    response.recover {
      case e @ BadQueryParameter(_) =>
        GuardrailResource.getDatasetsResponse
          .BadRequest(e.getMessage)

      case e: NumberFormatException =>
        GuardrailResource.getDatasetsResponse
          .BadRequest("ids must be numbers")
    }
  }

  override def getDataset(
    respond: GuardrailResource.getDatasetResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.getDatasetResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper
        .getLatestVisibleVersion(dataset)
        .flatMap {
          case Some(v) if v.status == PublishStatus.Unpublished =>
            DBIO.failed(DatasetUnpublishedException(dataset, v))
          case Some(v) => DBIO.successful(v)
          case None => DBIO.failed(NoDatasetException(dataset.id))
        }

      maybeDatasetPreview <- DBIO.from {
        claim
          .map {
            _.content match {
              case userClaim: UserClaim => {
                ports.pennsieveApiClient
                  .getDatasetPreviewers(
                    dataset.sourceOrganizationId,
                    dataset.sourceDatasetId
                  )
                  .value
                  .flatMap(_.fold(Future.failed, Future.successful))
                  .map(_.find(_.user.intId == userClaim.id.value))
              }
              case _ => Future.successful(None)
            }
          }
          .getOrElse(Future.successful(None))
      }

      dto <- publicDatasetDTO(dataset, version, maybeDatasetPreview)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.getDatasetResponse
          .OK(_)
      )
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.getDatasetResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.getDatasetResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  /**
    * The DOI prefix and suffix must be passed as separate path components
    * because Swagger requires `/` to represent a path division.
    */
  override def getDatasetByDoi(
    respond: GuardrailResource.getDatasetByDoiResponse.type
  )(
    doiPrefix: String,
    doiSuffix: String
  ): Future[GuardrailResource.getDatasetByDoiResponse] = {

    val doi = s"$doiPrefix/$doiSuffix"

    val query = for {
      (dataset, version) <- PublicDatasetVersionsMapper
        .getVersionByDoi(doi)

      dto <- publicDatasetDTO(dataset, version)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.getDatasetByDoiResponse
          .OK(_)
      )
      .recover {
        case NoDatasetForDoiException(_) =>
          GuardrailResource.getDatasetByDoiResponse
            .NotFound(doi)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.getDatasetByDoiResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  override def requestPreview(
    respond: GuardrailResource.requestPreviewResponse.type
  )(
    datasetId: Int,
    body: PreviewAccessRequest
  ): Future[GuardrailResource.requestPreviewResponse] = {
    claim match {
      case Some(c) => {
        c.content match {
          case userClaim: UserClaim => {
            val query = for {
              dataset <- PublicDatasetsMapper.getDataset(datasetId)
              r <- DBIO.from {
                ports.pennsieveApiClient
                  .requestPreview(
                    organizationId = dataset.sourceOrganizationId,
                    previewRequest = APIPreviewAccessRequest(
                      datasetId = dataset.sourceDatasetId,
                      userId = userClaim.id.value,
                      dataUseAgreementId = body.dataUseAgreementId
                    )
                  )
                  .value
                  .flatMap(
                    _.fold(
                      (e: HttpError) => {
                        e.statusCode match {
                          case StatusCodes.NotFound => {
                            Future.successful(
                              GuardrailResource.requestPreviewResponse
                                .NotFound("data use agreement not found")
                            )
                          }
                          case _ => Future.failed(e)
                        }
                      },
                      _ =>
                        Future
                          .successful(
                            GuardrailResource.requestPreviewResponse.OK
                          )
                    )
                  )
              }
            } yield r
            ports.db
              .run(query.transactionally)
              .recover {
                case NoDatasetException(id) =>
                  GuardrailResource.requestPreviewResponse
                    .NotFound(id.toString)
              }
          }
          case _ =>
            Future.successful(
              GuardrailResource.requestPreviewResponse
                .Unauthorized("not a user claim")
            )
        }
      }
      case _ =>
        Future.successful(
          GuardrailResource.requestPreviewResponse
            .Unauthorized("missing token")
        )
    }
  }

  override def getDatasetVersions(
    respond: GuardrailResource.getDatasetVersionsResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.getDatasetVersionsResponse] = {
    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      versions <- PublicDatasetVersionsMapper
        .getVersions(
          id = datasetId,
          status = Set(PublishSucceeded, Unpublished, EmbargoSucceeded)
        )
        .result
      contributorsMap <- PublicContributorsMapper.getDatasetContributors(
        versions.map((dataset, _))
      )

      collectionsMap <- PublicCollectionsMapper.getDatasetCollections(
        versions.map((dataset, _))
      )

      externalPublicationsMap <- PublicExternalPublicationsMapper
        .getExternalPublications(versions.map((dataset, _)))

      sponsorship <- SponsorshipsMapper.maybeGetByDataset(dataset)
      revisionsMap <- RevisionsMapper.getLatestRevisions(versions)

    } yield
      (
        dataset,
        versions,
        contributorsMap,
        sponsorship,
        revisionsMap,
        collectionsMap,
        externalPublicationsMap
      )

    ports.db
      .run(query.transactionally)
      .map {
        case (
            dataset,
            versions,
            contributorsMap,
            sponsorship,
            revisionsMap,
            collectionsMap,
            externalPublicationsMap
            ) =>
          GuardrailResource.getDatasetVersionsResponse
            .OK(
              versions
                .map(
                  version =>
                    models.PublicDatasetDTO(
                      dataset,
                      version,
                      contributorsMap
                        .get(dataset, version)
                        .getOrElse(Nil),
                      sponsorship,
                      revisionsMap.get(version).flatten,
                      collectionsMap
                        .get(dataset, version)
                        .getOrElse(Nil),
                      externalPublicationsMap
                        .get(dataset, version)
                        .getOrElse(Nil),
                      None
                    )
                )
                .toIndexedSeq
            )
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.getDatasetVersionsResponse
            .NotFound(datasetId.toString)
      }
  }

  override def getDatasetVersion(
    respond: GuardrailResource.getDatasetVersionResponse.type
  )(
    datasetId: Int,
    versionId: Int
  ): Future[GuardrailResource.getDatasetVersionResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )

      dto <- publicDatasetDTO(dataset, version)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.getDatasetVersionResponse
          .OK(_)
      )
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.getDatasetVersionResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.getDatasetVersionResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  override def getFile(
    respond: GuardrailResource.getFileResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    originalPath: String
  ): Future[GuardrailResource.getFileResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)

      pathNoSchemeNoBucket = originalPath
        .replace("s3://" + version.s3Bucket, "")
      noSlashPath = if (pathNoSchemeNoBucket.startsWith("/")) {
        pathNoSchemeNoBucket.replaceFirst("/", "")
      } else {
        pathNoSchemeNoBucket
      }
      path = if (!noSlashPath.startsWith(datasetId + "/" + versionId)) {
        datasetId + "/" + versionId + "/" + noSlashPath
      } else {
        noSlashPath
      }

      file <- PublicFilesMapper.getFile(version, S3Key.File(path))

    } yield (dataset, version, file)

    ports.db
      .run(query.transactionally)
      .map {
        case (ds, v, f) =>
          GuardrailResource.getFileResponse
            .OK(models.FileTreeNodeDTO(f))
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.getFileResponse
            .NotFound(datasetId.toString)
        case NoDatasetVersionException(_, _) =>
          GuardrailResource.getFileResponse
            .NotFound(versionId.toString)
        case NoFileException(_, _, _) =>
          GuardrailResource.getFileResponse
            .NotFound(originalPath)
        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.getFileResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
        case UnauthorizedException =>
          GuardrailResource.getFileResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.getFileResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  /**
    * This endpoint returns a raw HttpResponse because Guardrail does not
    * support streaming responses.
    */
  override def downloadDatasetVersion(
    respond: GuardrailResource.downloadDatasetVersionResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    downloadOrigin: Option[String]
  ): Future[HttpResponse] = {

    implicit val logContext = DiscoverLogContext(
      publicDatasetId = Some(datasetId),
      publicDatasetVersion = Some(versionId)
    )

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)
    } yield (dataset, version)

    ports.db
      .run(query.transactionally)
      .flatMap {
        case (dataset: PublicDataset, version: PublicDatasetVersion) =>
          if (version.size > ports.config.download.maxSize.toBytes)
            Future.failed(DatasetTooLargeException)
          else
            Future.successful((dataset, version))
      }
      .flatMap {
        case (dataset: PublicDataset, version: PublicDatasetVersion) => {
          downloadOrigin match {
            case Some(origin)
                if DownloadOrigin.withNameInsensitiveOption(origin).isEmpty =>
              Future.failed(UnknownOrigin(origin))
            case _ =>
              ports.db
                .run(
                  DatasetDownloadsMapper
                    .create(
                      dataset.id,
                      version.version,
                      DownloadOrigin
                        .withNameInsensitiveOption(downloadOrigin.getOrElse(""))
                    )
                )
                .map(_ => (dataset, version))
          }
        }
      }
      .map {
        case (dataset: PublicDataset, version: PublicDatasetVersion) => {
          val zipPrefix =
            s"Pennsieve-dataset-${dataset.id}-version-${version.version}"

          HttpResponse(
            entity = HttpEntity(
              ContentType.Binary(MediaTypes.`application/zip`),
              ports.s3StreamClient
                .datasetFilesSource(version, zipPrefix)
                .via(ZipStream())
                .throttle(
                  cost = ports.config.download.ratePerSecond.toBytes.toInt,
                  per = 1.seconds,
                  _.length
                )
                .recover {
                  case e: Throwable => {
                    ports.log.error("Stream error", e)
                    throw e
                  }
                }
            ),
            headers = List(
              new `Content-Disposition`(
                ContentDispositionTypes.attachment,
                Map("filename" -> s"$zipPrefix.zip")
              )
            )
          )
        }
      }
      .recoverWith {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.downloadDatasetVersionResponse.Gone)
            .to[HttpResponse]

        case DatasetTooLargeException =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .Forbidden("Dataset is too large to download directly.")
          ).to[HttpResponse]

        case UnauthorizedException =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .Unauthorized("Dataset is under embargo")
          ).to[HttpResponse]

        case DatasetUnderEmbargo =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .Forbidden("Dataset is under embargo")
          ).to[HttpResponse]
      }
  }

  /**
    * This endpoint returns a raw HttpResponse because Guardrail does not
    * support streaming responses.
    */
  override def getDatasetMetadata(
    respond: GuardrailResource.getDatasetMetadataResponse.type
  )(
    datasetId: Int,
    versionId: Int
  ): Future[HttpResponse] = {
    implicit val logContext = DiscoverLogContext(
      publicDatasetId = Some(datasetId),
      publicDatasetVersion = Some(versionId)
    )

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)
    } yield (dataset, version)

    ports.db
      .run(query.transactionally)
      .flatMap {
        case (dataset: PublicDataset, version: PublicDatasetVersion) =>
          ports.s3StreamClient
            .datasetMetadataSource(version)
            .map {
              case ((source, contentLength)) =>
                HttpResponse(
                  entity = HttpEntity(
                    ContentTypes.`application/json`,
                    contentLength,
                    source
                  )
                )
            }
      }
      .recoverWith {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          Marshal(
            GuardrailResource.getDatasetMetadataResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.downloadDatasetVersionResponse.Gone)
            .to[HttpResponse]

        case UnauthorizedException =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .Unauthorized("Dataset is under embargo")
          ).to[HttpResponse]

        case DatasetUnderEmbargo =>
          Marshal(
            GuardrailResource.downloadDatasetVersionResponse
              .Forbidden("Dataset is under embargo")
          ).to[HttpResponse]
      }
  }

  override def browseFiles(
    respond: GuardrailResource.browseFilesResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    path: Option[String],
    limit: Option[Int],
    offset: Option[Int]
  ): Future[GuardrailResource.browseFilesResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)

      (totalCount, files) <- PublicFilesMapper.childrenOf(
        version,
        path,
        limit = limit.getOrElse(defaultFileLimit),
        offset = offset.getOrElse(defaultFileOffset)
      )
    } yield (totalCount, files)

    ports.db
      .run(query)
      .map {
        case (totalCount, files) =>
          GuardrailResource.browseFilesResponse.OK(
            FileTreePage(
              totalCount = totalCount.value,
              limit = limit.getOrElse(defaultFileLimit),
              offset = offset.getOrElse(defaultFileOffset),
              files = files.map(FileTreeNodeDTO.apply).to[IndexedSeq]
            )
          )
      }
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.browseFilesResponse
            .NotFound(datasetId.toString)
        case UnauthorizedException =>
          GuardrailResource.browseFilesResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.browseFilesResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  override def downloadManifest(
    respond: GuardrailResource.downloadManifestResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    body: DownloadRequest
  ): Future[GuardrailResource.downloadManifestResponse] = {

    val pathsMatchRootPath = body.rootPath
      .map(rp => body.paths.count(p => p.startsWith(rp)))
      .getOrElse(body.paths.length) == body.paths.length
    if (!pathsMatchRootPath) {
      Future.successful(
        GuardrailResource.downloadManifestResponse
          .BadRequest(
            "if root path is specified, all paths must begin with root path"
          )
      )
    } else {
      val query = for {
        dataset <- PublicDatasetsMapper.getDataset(datasetId)
        version <- PublicDatasetVersionsMapper.getVisibleVersion(
          dataset,
          versionId
        )
        _ <- authorizeIfUnderEmbargo(dataset, version)
        downloadDtos <- PublicFilesMapper.getFileDownloadsMatchingPaths(
          version,
          body.paths
        )
      } yield (downloadDtos, dataset, version)

      ports.db
        .run(query)
        .flatMap {
          case (
              downloadDtos: Seq[FileDownloadDTO],
              dataset: PublicDataset,
              version: PublicDatasetVersion
              ) => {
            val (count, size) = downloadDtos.foldLeft((0, 0L)) {
              case ((c, s), dto) => (c + 1, s + dto.size)
            }
            if (size > ports.config.download.maxSize.toBytes) {
              Future.failed(DatasetTooLargeException)
            } else {
              Future.successful((downloadDtos, dataset, version, count, size))
            }
          }
        }
        .flatMap {
          case (
              downloadDtos: Seq[FileDownloadDTO],
              dataset: PublicDataset,
              version: PublicDatasetVersion,
              count: Int,
              size: Long
              ) =>
            ports.db
              .run(
                PublicDatasetVersionsMapper
                  .increaseFilesDownloadCounter(dataset, version.version, count)
              )
              .map(_ => (downloadDtos, dataset, version, count, size))
        }
        .map {

          case (
              downloadDtos: Seq[FileDownloadDTO],
              _: PublicDataset,
              _: PublicDatasetVersion,
              count: Int,
              size: Long
              ) => {
            GuardrailResource.downloadManifestResponse.OK(
              DownloadResponse(
                DownloadResponseHeader(count, size),
                downloadDtos
                  .map(
                    _.toDownloadResponseItem(
                      ports.s3StreamClient,
                      body.rootPath
                    )
                  )
                  .toIndexedSeq
              )
            )
          }
        }
        .recover {
          case DatasetTooLargeException =>
            GuardrailResource.downloadManifestResponse
              .Forbidden("requested files are too large to download")
          case UnauthorizedException =>
            GuardrailResource.downloadManifestResponse.Unauthorized(
              "Dataset is under embargo"
            )
          case DatasetUnderEmbargo =>
            GuardrailResource.downloadManifestResponse.Forbidden(
              "Dataset is under embargo"
            )
        }
    }
  }

  /**
    * Proxy requests to API to get data use agreements. This is required because
    * users who need to see data use agreements do not have access to them in
    * the platform.
    *
    * Note: this returns the current agreement for the dataset, not necessarily
    * the one signed by a user.
    */
  override def getDataUseAgreement(
    respond: GuardrailResource.getDataUseAgreementResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.getDataUseAgreementResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper
        .getLatestVisibleVersion(dataset)
        .flatMap {
          case Some(v) if v.status == PublishStatus.Unpublished =>
            DBIO.failed(DatasetUnpublishedException(dataset, v))
          case Some(v) => DBIO.successful(v)
          case None => DBIO.failed(NoDatasetException(dataset.id))
        }

      agreement <- DBIO.from(
        ports.pennsieveApiClient
          .getDataUseAgreement(
            sourceOrganizationId = dataset.sourceOrganizationId,
            sourceDatasetId = dataset.sourceDatasetId
          )
          .value
          .flatMap(_.fold(Future.failed, Future.successful))
      )

    } yield (agreement, dataset)

    ports.db
      .run(query.transactionally)
      .map {
        case (Some(agreement), dataset) =>
          GuardrailResource.getDataUseAgreementResponse
            .OK(
              definitions.DataUseAgreementDTO(
                id = agreement.id,
                name = agreement.name,
                body = agreement.body,
                organizationId = dataset.sourceOrganizationId
              )
            )

        case (None, _) =>
          GuardrailResource.getDataUseAgreementResponse.NoContent
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.getDataUseAgreementResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.getDataUseAgreementResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  /**
    * This returns a raw response because of Guardrail limitations: it is not
    * possible to set a Content-Disposition response header.
    */
  override def downloadDataUseAgreement(
    respond: GuardrailResource.downloadDataUseAgreementResponse.type
  )(
    datasetId: Int
  ): Future[HttpResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper
        .getLatestVisibleVersion(dataset)
        .flatMap {
          case Some(v) if v.status == PublishStatus.Unpublished =>
            DBIO.failed(DatasetUnpublishedException(dataset, v))
          case Some(v) => DBIO.successful(v)
          case None => DBIO.failed(NoDatasetException(dataset.id))
        }

      agreement <- DBIO.from(
        ports.pennsieveApiClient
          .getDataUseAgreement(
            sourceOrganizationId = dataset.sourceOrganizationId,
            sourceDatasetId = dataset.sourceDatasetId
          )
          .value
          .flatMap(_.fold(Future.failed, Future.successful))
      )
    } yield (agreement, dataset, version)

    ports.db
      .run(query.transactionally)
      .flatMap {
        case (Some(agreement), dataset, version) =>
          Future.successful(
            HttpResponse(
              entity = HttpEntity(
                ContentTypes.`text/plain(UTF-8)`,
                formatAgreement(dataset, version, agreement.body)
              ),
              headers = List(
                new `Content-Disposition`(
                  ContentDispositionTypes.attachment,
                  Map(
                    "filename" -> s"Pennsieve-dataset-${dataset.id}-data-use-agreement.txt"
                  )
                )
              )
            )
          )

        case (None, _, _) =>
          Marshal(GuardrailResource.downloadDataUseAgreementResponse.NoContent)
            .to[HttpResponse]
      }
      .recoverWith {
        case NoDatasetException(_) =>
          Marshal(
            GuardrailResource.downloadDataUseAgreementResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]: Future[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.downloadDataUseAgreementResponse.Gone)
            .to[HttpResponse]: Future[HttpResponse]
      }
  }

  private def formatAgreement(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    agreement: String
  ): String =
    s"""
       |${dataset.name} (${version.doi})
       |
       |${LocalDate.now}
       |
       |$agreement
       |""".stripMargin.trim
}

object DatasetHandler {

  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    logRequestAndResponse(ports) {
      optionalHeaderValue {
        case header @ Authorization(OAuth2BearerToken(_)) => Some(header)
        case _ => None
      } {
        case authorization @ Some(_) =>
          authenticateJwt(system.name)(ports.jwt) { claim =>
            GuardrailResource.routes(
              new DatasetHandler(ports, authorization, claim.some)
            )
          }
        case _ =>
          GuardrailResource.routes(new DatasetHandler(ports, None, None))
      }
    }
}
