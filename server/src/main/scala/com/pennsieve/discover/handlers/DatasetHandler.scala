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
  AssetTreeNodeDto,
  AssetTreePage,
  DownloadRequest,
  DownloadResponse,
  DownloadResponseHeader,
  FileTreePage,
  PreviewAccessRequest,
  PublicDatasetDto
}
import com.pennsieve.models.{ DatasetType, PublishStatus }
import com.pennsieve.models.PublishStatus.{
  EmbargoSucceeded,
  NotPublished,
  PublishSucceeded,
  Unpublished
}

import scala.concurrent.duration.{ DurationInt, SECONDS }
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
    preview: Option[DatasetPreview] = None,
    release: Option[PublicDatasetRelease] = None
  ): DBIO[PublicDatasetDto] =
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
        preview,
        release = release
      )

  override def getDatasets(
    respond: GuardrailResource.GetDatasetsResponse.type
  )(
    limit: Option[Int],
    offset: Option[Int],
    ids: Option[Iterable[String]],
    tags: Option[Iterable[String]],
    embargo: Option[Boolean],
    datasetType: Option[String],
    orderBy: Option[String],
    orderDirection: Option[String]
  ): Future[GuardrailResource.GetDatasetsResponse] = {

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

      datasetTypeFilter = datasetType match {
        case Some(t) => Some(DatasetType.withName(t))
        case None => None
      }

      pagedResult <- ports.db
        .run(
          PublicDatasetVersionsMapper.getPagedDatasets(
            tags = tags.map(_.toList),
            embargo = embargo,
            limit = limit.getOrElse(defaultDatasetLimit),
            offset = offset.getOrElse(defaultDatasetOffset),
            orderBy = orderBy,
            orderDirection = orderDirection,
            ids = intIds,
            datasetType = datasetTypeFilter
          )
        )
    } yield
      GuardrailResource.GetDatasetsResponse
        .OK(DatasetsPage.apply(pagedResult))

    response.recover {
      case e @ BadQueryParameter(_) =>
        GuardrailResource.GetDatasetsResponse
          .BadRequest(e.getMessage)

      case e: NumberFormatException =>
        GuardrailResource.GetDatasetsResponse
          .BadRequest("ids must be numbers")
    }
  }

  override def getDataset(
    respond: GuardrailResource.GetDatasetResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.GetDatasetResponse] = {

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

      release <- PublicDatasetReleaseMapper.get(dataset.id, version.version)

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

      dto <- publicDatasetDTO(dataset, version, maybeDatasetPreview, release)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.GetDatasetResponse
          .OK(_)
      )
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.GetDatasetResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.GetDatasetResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  /**
    * The DOI prefix and suffix must be passed as separate path components
    * because Swagger requires `/` to represent a path division.
    */
  override def getDatasetByDoi(
    respond: GuardrailResource.GetDatasetByDoiResponse.type
  )(
    doiPrefix: String,
    doiSuffix: String
  ): Future[GuardrailResource.GetDatasetByDoiResponse] = {

    val doi = s"$doiPrefix/$doiSuffix"

    val query = for {
      (dataset, version) <- PublicDatasetVersionsMapper
        .getVersionByDoi(doi)

      release <- PublicDatasetReleaseMapper.get(dataset.id, version.version)

      dto <- publicDatasetDTO(dataset, version, release = release)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.GetDatasetByDoiResponse
          .OK(_)
      )
      .recover {
        case NoDatasetForDoiException(_) =>
          GuardrailResource.GetDatasetByDoiResponse
            .NotFound(doi)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.GetDatasetByDoiResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  override def requestPreview(
    respond: GuardrailResource.RequestPreviewResponse.type
  )(
    datasetId: Int,
    body: PreviewAccessRequest
  ): Future[GuardrailResource.RequestPreviewResponse] = {
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
                              GuardrailResource.RequestPreviewResponse
                                .NotFound("data use agreement not found")
                            )
                          }
                          case _ => Future.failed(e)
                        }
                      },
                      _ =>
                        Future
                          .successful(
                            GuardrailResource.RequestPreviewResponse.OK
                          )
                    )
                  )
              }
            } yield r
            ports.db
              .run(query.transactionally)
              .recover {
                case NoDatasetException(id) =>
                  GuardrailResource.RequestPreviewResponse
                    .NotFound(id.toString)
              }
          }
          case _ =>
            Future.successful(
              GuardrailResource.RequestPreviewResponse
                .Unauthorized("not a user claim")
            )
        }
      }
      case _ =>
        Future.successful(
          GuardrailResource.RequestPreviewResponse
            .Unauthorized("missing token")
        )
    }
  }

  override def getDatasetVersions(
    respond: GuardrailResource.GetDatasetVersionsResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.GetDatasetVersionsResponse] = {
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
          GuardrailResource.GetDatasetVersionsResponse
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
                .toVector
            )
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.GetDatasetVersionsResponse
            .NotFound(datasetId.toString)
      }
  }

  override def getDatasetVersion(
    respond: GuardrailResource.GetDatasetVersionResponse.type
  )(
    datasetId: Int,
    versionId: Int
  ): Future[GuardrailResource.GetDatasetVersionResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )

      release <- PublicDatasetReleaseMapper.get(dataset.id, version.version)

      dto <- publicDatasetDTO(dataset, version, release = release)

    } yield dto

    ports.db
      .run(query.transactionally)
      .map(
        GuardrailResource.GetDatasetVersionResponse
          .OK(_)
      )
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.GetDatasetVersionResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.GetDatasetVersionResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  override def getFile(
    respond: GuardrailResource.GetFileResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    originalPath: String
  ): Future[GuardrailResource.GetFileResponse] = {

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
      path = version.migrated match {
        case true =>
          if (!noSlashPath.startsWith(s"$datasetId")) {
            s"$datasetId/$noSlashPath"
          } else {
            noSlashPath
          }
        case false =>
          if (!noSlashPath.startsWith(s"$datasetId/$versionId")) {
            s"$datasetId/$versionId/$noSlashPath"
          } else {
            noSlashPath
          }
      }

      file <- version.migrated match {
        case false =>
          PublicFilesMapper.getFile(version, S3Key.File(path))
        case true =>
          PublicFileVersionsMapper.getFile(version, S3Key.File(path))
      }

    } yield (dataset, version, file)

    ports.db
      .run(query.transactionally)
      .map {
        case (ds, v, f) =>
          GuardrailResource.GetFileResponse
            .OK(models.FileTreeNodeDTO(f))
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.GetFileResponse
            .NotFound(datasetId.toString)
        case NoDatasetVersionException(_, _) =>
          GuardrailResource.GetFileResponse
            .NotFound(versionId.toString)
        case NoFileException(_, _, _) =>
          GuardrailResource.GetFileResponse
            .NotFound(originalPath)
        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.GetFileResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
        case UnauthorizedException =>
          GuardrailResource.GetFileResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.GetFileResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  /**
    * This endpoint returns a raw HttpResponse because Guardrail does not
    * support streaming responses.
    */
  override def downloadDatasetVersion(
    respond: GuardrailResource.DownloadDatasetVersionResponse.type
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
                .datasetFilesSource(version, zipPrefix, Some(ports.db))
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
            GuardrailResource.DownloadDatasetVersionResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.DownloadDatasetVersionResponse.Gone)
            .to[HttpResponse]

        case DatasetTooLargeException =>
          Marshal(
            GuardrailResource.DownloadDatasetVersionResponse
              .Forbidden("Dataset is too large to download directly.")
          ).to[HttpResponse]

        case UnauthorizedException =>
          Marshal(
            GuardrailResource.DownloadDatasetVersionResponse
              .Unauthorized("Dataset is under embargo")
          ).to[HttpResponse]

        case DatasetUnderEmbargo =>
          Marshal(
            GuardrailResource.DownloadDatasetVersionResponse
              .Forbidden("Dataset is under embargo")
          ).to[HttpResponse]
      }
  }

  /**
    * This endpoint returns a raw HttpResponse because Guardrail does not
    * support streaming responses.
    */
  override def getDatasetMetadata(
    respond: GuardrailResource.GetDatasetMetadataResponse.type
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
      file <- version.migrated match {
        case true =>
          PublicFileVersionsMapper.getFile(
            version,
            DatasetMetadata.metadataKey(version)
          )
        case false =>
          PublicFilesMapper.getFile(
            version,
            DatasetMetadata.metadataKey(version)
          )
      }
    } yield (dataset, version, file)

    ports.db
      .run(query.transactionally)
      .flatMap {
        case (
            dataset: PublicDataset,
            version: PublicDatasetVersion,
            file: FileTreeNode.File
            ) =>
          ports.s3StreamClient
            .datasetMetadataSource(file)
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
        case (_, _, _) => ??? // TODO: do something better here
      }
      .recoverWith {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          Marshal(
            GuardrailResource.GetDatasetMetadataResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.DownloadDatasetVersionResponse.Gone)
            .to[HttpResponse]

        case UnauthorizedException =>
          Marshal(
            GuardrailResource.DownloadDatasetVersionResponse
              .Unauthorized("Dataset is under embargo")
          ).to[HttpResponse]

        case DatasetUnderEmbargo =>
          Marshal(
            GuardrailResource.DownloadDatasetVersionResponse
              .Forbidden("Dataset is under embargo")
          ).to[HttpResponse]
      }
  }

  override def browseFiles(
    respond: GuardrailResource.BrowseFilesResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    path: Option[String],
    limit: Option[Int],
    offset: Option[Int]
  ): Future[GuardrailResource.BrowseFilesResponse] = {

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)

      (totalCount, files) <- version.migrated match {
        case false =>
          PublicFilesMapper.childrenOf(
            version,
            path,
            limit = limit.getOrElse(defaultFileLimit),
            offset = offset.getOrElse(defaultFileOffset)
          )
        case true =>
          PublicFileVersionsMapper.childrenOf(
            version,
            path,
            limit = limit.getOrElse(defaultFileLimit),
            offset = offset.getOrElse(defaultFileOffset)
          )
      }

    } yield (totalCount, files)

    ports.db
      .run(query)
      .map {
        case (totalCount, files) =>
          GuardrailResource.BrowseFilesResponse.OK(
            FileTreePage(
              totalCount = totalCount.value,
              limit = limit.getOrElse(defaultFileLimit),
              offset = offset.getOrElse(defaultFileOffset),
              files = files.map(FileTreeNodeDTO.apply).to(Vector)
            )
          )
      }
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.BrowseFilesResponse
            .NotFound(datasetId.toString)
        case UnauthorizedException =>
          GuardrailResource.BrowseFilesResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.BrowseFilesResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  override def browseAssets(
    respond: GuardrailResource.BrowseAssetsResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    path: Option[String],
    limit: Option[Int],
    offset: Option[Int]
  ): Future[GuardrailResource.BrowseAssetsResponse] = {
    def valid(path: Option[String]): Option[String] =
      path match {
        case None => None
        case Some(path) =>
          path.length match {
            case 0 => None
            case _ => Some(path)
          }
      }

    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)

      (totalCount, assets) <- PublicDatasetReleaseAssetMapper
        .childrenOf(
          version,
          valid(path),
          limit = limit.getOrElse(defaultFileLimit),
          offset = offset.getOrElse(defaultFileOffset)
        )
    } yield (totalCount, assets)

    ports.db
      .run(query)
      .map {
        case (totalCount, assets) =>
          GuardrailResource.BrowseAssetsResponse.OK(
            AssetTreePage(
              limit = limit.getOrElse(defaultFileLimit),
              offset = offset.getOrElse(defaultFileOffset),
              totalCount = totalCount.value,
              assets = assets.map(AssetTreeNodeDTO.apply).to(Vector)
            )
          )
      }
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.BrowseAssetsResponse
            .NotFound(datasetId.toString)
        case UnauthorizedException =>
          GuardrailResource.BrowseAssetsResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.BrowseAssetsResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  override def browseAllAssets(
    respond: GuardrailResource.BrowseAllAssetsResponse.type
  )(
    datasetId: Int,
    versionId: Int
  ): Future[GuardrailResource.BrowseAllAssetsResponse] = {
    val query = for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      version <- PublicDatasetVersionsMapper.getVisibleVersion(
        dataset,
        versionId
      )
      _ <- authorizeIfUnderEmbargo(dataset, version)

      (totalCount, assets) <- PublicDatasetReleaseAssetMapper
        .fullTree(version)
    } yield (totalCount, assets)

    ports.db
      .run(query)
      .map {
        case (totalCount, assets) =>
          GuardrailResource.BrowseAllAssetsResponse.OK(
            AssetTreePage(
              limit = -1,
              offset = -1,
              totalCount = totalCount.value,
              assets = assets.map(AssetTreeNodeDTO.apply).to(Vector)
            )
          )
      }
      .recover {
        case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
          GuardrailResource.BrowseAllAssetsResponse
            .NotFound(datasetId.toString)
        case UnauthorizedException =>
          GuardrailResource.BrowseAllAssetsResponse.Unauthorized(
            "Dataset is under embargo"
          )
        case DatasetUnderEmbargo =>
          GuardrailResource.BrowseAllAssetsResponse.Forbidden(
            "Dataset is under embargo"
          )
      }
  }

  override def downloadManifest(
    respond: GuardrailResource.DownloadManifestResponse.type
  )(
    datasetId: Int,
    versionId: Int,
    body: DownloadRequest
  ): Future[GuardrailResource.DownloadManifestResponse] = {

    val pathsMatchRootPath = body.rootPath
      .map(rp => body.paths.count(p => p.startsWith(rp)))
      .getOrElse(body.paths.length) == body.paths.length
    if (!pathsMatchRootPath) {
      Future.successful(
        GuardrailResource.DownloadManifestResponse
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
        downloadDtos <- version.migrated match {
          case true =>
            PublicFileVersionsMapper.getFileDownloadsMatchingPaths(
              version,
              body.paths
            )
          case false =>
            PublicFilesMapper.getFileDownloadsMatchingPaths(version, body.paths)
        }
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
            GuardrailResource.DownloadManifestResponse.OK(
              DownloadResponse(
                DownloadResponseHeader(count, size),
                downloadDtos
                  .map(
                    _.toDownloadResponseItem(
                      ports.s3StreamClient,
                      body.rootPath
                    )
                  )
                  .toVector
              )
            )
          }
        }
        .recover {
          case DatasetTooLargeException =>
            GuardrailResource.DownloadManifestResponse
              .Forbidden("requested files are too large to download")
          case UnauthorizedException =>
            GuardrailResource.DownloadManifestResponse.Unauthorized(
              "Dataset is under embargo"
            )
          case DatasetUnderEmbargo =>
            GuardrailResource.DownloadManifestResponse.Forbidden(
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
    respond: GuardrailResource.GetDataUseAgreementResponse.type
  )(
    datasetId: Int
  ): Future[GuardrailResource.GetDataUseAgreementResponse] = {

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
          GuardrailResource.GetDataUseAgreementResponse
            .OK(
              definitions.DataUseAgreementDto(
                id = agreement.id,
                name = agreement.name,
                body = agreement.body,
                organizationId = dataset.sourceOrganizationId
              )
            )

        case (None, _) =>
          GuardrailResource.GetDataUseAgreementResponse.NoContent
      }
      .recover {
        case NoDatasetException(_) =>
          GuardrailResource.GetDataUseAgreementResponse
            .NotFound(datasetId.toString)

        case DatasetUnpublishedException(dataset, version) =>
          GuardrailResource.GetDataUseAgreementResponse.Gone(
            models.TombstoneDTO(dataset, version)
          )
      }
  }

  /**
    * This returns a raw response because of Guardrail limitations: it is not
    * possible to set a Content-Disposition response header.
    */
  override def downloadDataUseAgreement(
    respond: GuardrailResource.DownloadDataUseAgreementResponse.type
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
          Marshal(GuardrailResource.DownloadDataUseAgreementResponse.NoContent)
            .to[HttpResponse]
      }
      .recoverWith {
        case NoDatasetException(_) =>
          Marshal(
            GuardrailResource.DownloadDataUseAgreementResponse
              .NotFound(datasetId.toString)
          ).to[HttpResponse]: Future[HttpResponse]

        case DatasetUnpublishedException(_, _) =>
          Marshal(GuardrailResource.DownloadDataUseAgreementResponse.Gone)
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
