// Copyright (c) 2026 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ PathMatchers, Route }
import com.pennsieve.discover._
import com.pennsieve.discover.clients.AuthorizationClient
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db.{
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFileVersionsMapper,
  PublicFilesMapper
}
import com.pennsieve.discover.logging.logRequestAndResponse
import com.pennsieve.discover.models.{
  FileDownloadDTO,
  PublicDataset,
  PublicDatasetVersion
}
import com.pennsieve.discover.server.definitions.{
  DownloadResolveFile,
  DownloadResolveRequest,
  DownloadResolveResponse,
  DownloadResolveSkipped,
  DownloadResolveVersion
}
import com.pennsieve.discover.server.resolve.{
  ResolveHandler => GuardrailHandler,
  ResolveResource => GuardrailResource
}
import com.pennsieve.models.PublishStatus

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64
import scala.concurrent.{ ExecutionContext, Future }

/**
  * Resolves a selection of a published version for download-service
  * (download-service docs/public-downloads.md): which files, where, and
  * whether the caller may download them. download-service checks the answer
  * and signs; this route never signs, and never writes.
  *
  * Fails closed: an embargoed version needs the caller's credential and
  * authorization-service's OK, anything unexpected is a 5xx rather than a
  * decision, and files are only returned with "allowed".
  */
class ResolveHandler(
  ports: Ports,
  userAuthorization: Option[Authorization]
)(implicit
  executionContext: ExecutionContext,
  system: ActorSystem
) extends GuardrailHandler {
  import ResolveHandler._

  override def downloadResolve(
    respond: GuardrailResource.DownloadResolveResponse.type
  )(
    datasetId: Int,
    version: String,
    body: DownloadResolveRequest
  ): Future[GuardrailResource.DownloadResolveResponse] = {
    val limit = body.limit.filter(l => l > 0 && l <= PageSize).getOrElse(PageSize)
    val rootPathOk =
      body.rootPath.forall(rp => body.paths.forall(_.startsWith(rp)))

    decodeCursor(body.cursor) match {
      case Left(_) =>
        Future.successful(
          GuardrailResource.DownloadResolveResponse
            .BadRequest("cursor is not one this service returned")
        )
      case Right(_) if !rootPathOk =>
        Future.successful(
          GuardrailResource.DownloadResolveResponse.BadRequest(
            "if root path is specified, all paths must begin with root path"
          )
        )
      case Right(after) =>
        ports.db
          .run(findVersion(datasetId, version))
          .flatMap {
            case (dataset, v) =>
              decide(dataset, v).flatMap {
                case Allowed => files(v, body, after, limit)
                case decision =>
                  Future.successful(withoutFiles(decision, Some(v)))
              }
          }
          .recover {
            case NoDatasetException(_) | NoDatasetVersionException(_, _) =>
              withoutFiles(NotFound, None)
            case DatasetUnpublishedException(_, v) =>
              withoutFiles(Unpublished, Some(v))
          }
    }
  }

  private def findVersion(
    datasetId: Int,
    version: String
  ): DBIO[(PublicDataset, PublicDatasetVersion)] =
    for {
      dataset <- PublicDatasetsMapper.getDataset(datasetId)
      v <- version match {
        case "latest" =>
          PublicDatasetVersionsMapper.getLatestVisibleVersion(dataset).flatMap {
            case None => DBIO.failed(NoDatasetVersionException(datasetId, 0))
            case Some(v) if v.status == PublishStatus.Unpublished =>
              DBIO.failed(DatasetUnpublishedException(dataset, v))
            case Some(v) => DBIO.successful(v)
          }
        case n =>
          n.toIntOption match {
            case Some(number) if number > 0 =>
              PublicDatasetVersionsMapper.getVisibleVersion(dataset, number)
            case _ => DBIO.failed(NoDatasetVersionException(datasetId, 0))
          }
      }
    } yield (dataset, v)

  /**
    * As authorizeIfUnderEmbargo, with the credential taken only from the
    * forwarded header: this request's own Authorization is the resolve
    * token. A failed authorization call is an error, never a decision.
    */
  private def decide(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  ): Future[String] =
    if (!version.underEmbargo) Future.successful(Allowed)
    else
      userAuthorization match {
        case None => Future.successful(SignIn)
        case Some(authorization) =>
          ports.authorizationClient
            .authorizeEmbargoPreview(
              organizationId = dataset.sourceOrganizationId,
              datasetId = dataset.sourceDatasetId,
              authorization = authorization
            )
            .value
            .flatMap {
              case Right(AuthorizationClient.EmbargoAuthorization.OK) =>
                Future.successful(Allowed)
              case Right(AuthorizationClient.EmbargoAuthorization.Unauthorized) =>
                Future.successful(SignIn)
              case Right(AuthorizationClient.EmbargoAuthorization.Forbidden) =>
                Future.successful(Forbidden)
              case Left(error) => Future.failed(error)
            }
      }

  private def files(
    version: PublicDatasetVersion,
    body: DownloadResolveRequest,
    after: String,
    limit: Int
  ): Future[GuardrailResource.DownloadResolveResponse] = {
    val query = if (version.migrated) {
      PublicFileVersionsMapper.getFileDownloadsMatchingPaths(version, body.paths)
    } else {
      PublicFilesMapper.getFileDownloadsMatchingPaths(version, body.paths)
    }
    ports.db.run(query).map { dtos =>
      // Migrated files must be pinned to an object version; one recorded
      // as missing can't be served safely.
      val (skipped, servable) = dtos.partition(
        d =>
          version.migrated && d.s3Version
            .forall(s => s.isEmpty || s == MissingVersion)
      )
      val sorted = servable.sortBy(_.s3Key.value)
      val rest = sorted.dropWhile(_.s3Key.value <= after)
      val page = rest.take(limit)
      val next =
        if (rest.length > limit) Some(encodeCursor(page.last.s3Key.value))
        else None

      GuardrailResource.DownloadResolveResponse.OK(
        DownloadResolveResponse(
          decision = Allowed,
          version = versionOf(version, sorted.map(_.size).sum, sorted.length),
          files = page.map(fileOf(_, body.rootPath)).toVector,
          // Reported once, with the first page.
          skipped =
            if (after.isEmpty)
              skipped.map(skippedOf(_, body.rootPath)).toVector
            else Vector.empty,
          next = next
        )
      )
    }
  }

  private def withoutFiles(
    decision: String,
    version: Option[PublicDatasetVersion]
  ): GuardrailResource.DownloadResolveResponse =
    GuardrailResource.DownloadResolveResponse.OK(
      DownloadResolveResponse(
        decision = decision,
        version = version
          .map(versionOf(_, 0, 0))
          .getOrElse(
            DownloadResolveVersion(
              number = 0,
              status = "",
              embargoed = false,
              legacy = false,
              size = 0,
              fileCount = 0
            )
          ),
        files = Vector.empty,
        skipped = Vector.empty,
        next = None
      )
    )
}

object ResolveHandler {
  val UserAuthorizationHeader = "X-Pennsieve-User-Authorization"

  val PageSize = 1000

  val Allowed = "allowed"
  val SignIn = "sign_in"
  val Forbidden = "forbidden"
  val NotFound = "not_found"
  val Unpublished = "unpublished"

  /** What PublicFileVersionsMapper stores when a manifest had no version. */
  val MissingVersion = "missing"

  private val CursorPrefix = "v1:"

  def encodeCursor(key: String): String =
    Base64.getUrlEncoder.withoutPadding
      .encodeToString((CursorPrefix + key).getBytes(UTF_8))

  def decodeCursor(cursor: Option[String]): Either[String, String] =
    cursor.filter(_.nonEmpty) match {
      case None => Right("")
      case Some(c) =>
        scala.util
          .Try(new String(Base64.getUrlDecoder.decode(c), UTF_8))
          .toOption
          .filter(_.startsWith(CursorPrefix))
          .map(_.stripPrefix(CursorPrefix))
          .toRight("invalid cursor")
    }

  def versionOf(
    v: PublicDatasetVersion,
    size: Long,
    count: Int
  ): DownloadResolveVersion =
    DownloadResolveVersion(
      number = v.version,
      status = v.status.entryName,
      embargoed = v.underEmbargo,
      legacy = !v.migrated,
      size = size,
      fileCount = count
    )

  private def pathOf(d: FileDownloadDTO, rootPath: Option[String]) =
    rootPath
      .map(rp => d.truncatePath(rp.split("/").toIndexedSeq, d.path))
      .getOrElse(d.path)
      .toVector

  def fileOf(d: FileDownloadDTO, rootPath: Option[String]): DownloadResolveFile =
    DownloadResolveFile(
      path = pathOf(d, rootPath),
      fileName = d.name,
      bucket = d.s3Bucket.value,
      key = d.s3Key.value,
      s3Version = d.s3Version.filter(_.nonEmpty),
      size = d.size,
      sha256 = d.sha256
    )

  def skippedOf(
    d: FileDownloadDTO,
    rootPath: Option[String]
  ): DownloadResolveSkipped =
    DownloadResolveSkipped(
      path = pathOf(d, rootPath),
      fileName = d.name,
      reason = "no_version"
    )

  /**
    * Only the resolve path, and only with a valid resolve token. The check is
    * scoped to the path so the other internal routes, tried after this one,
    * are unaffected.
    */
  def routes(
    ports: Ports
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Route =
    rawPathPrefixTest(
      PathMatchers.Slash ~ "datasets" / PathMatchers.IntNumber / "versions" / PathMatchers.Segment / "download-resolve"
    ) { (_, _) =>
      logRequestAndResponse(ports) {
        optionalHeaderValueByName("Authorization") { serviceAuthorization =>
          val verified = for {
            raw <- serviceAuthorization.toRight("missing token")
            token <- ResolveToken.bearer(raw).toRight("malformed token")
            _ <- ResolveToken.verify(
              token,
              ports.config.downloadResolve.key.getOrElse(""),
              Instant.now()
            )
          } yield ()
          verified match {
            case Left(reason) =>
              ports.logger.noContext.warn(s"download-resolve refused: $reason")
              complete(StatusCodes.Unauthorized, "invalid resolve token")
            case Right(()) =>
              optionalHeaderValueByName(UserAuthorizationHeader) { user =>
                val forwarded = user
                  .flatMap(ResolveToken.bearer)
                  .map(t => Authorization(OAuth2BearerToken(t)))
                GuardrailResource.routes(
                  new ResolveHandler(ports, forwarded)
                )
              }
          }
        }
      }
    }
}
