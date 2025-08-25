// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.{ Authorization, OAuth2BearerToken }
import com.pennsieve.discover.clients.LambdaClient
import com.pennsieve.discover.db.PublicDatasetVersionsMapper
import com.pennsieve.discover.logging.DiscoverLogContext

import java.time.temporal.ChronoUnit
import com.pennsieve.discover.models.{
  DatasetDownload,
  S3Bucket,
  S3CleanupStage
}
import com.pennsieve.doi.models.{ DoiDTO, DoiState }
import com.pennsieve.models._
import org.apache.commons.lang3.StringUtils
import software.amazon.awssdk.services.lambda.model.InvokeResponse

import scala.concurrent.{ ExecutionContext, Future }

package object utils {

  /**
    * Join a path, removing intervening whitespace and leading slashes.
    */
  def joinPath(pathPrefix: String, pathSuffix: String): String =
    StringUtils.removeStart(
      stripSlashes(pathPrefix) + "/" +
        stripSlashes(pathSuffix),
      "/"
    )

  def joinPath(paths: String*): String =
    paths.fold("")(joinPath)

  def stripSlashes(s: String): String =
    s.replaceFirst("^[/]+", "").replaceFirst("[/]+$", "")

  /**
    * Convert a string to a Pennsieve FileType.
    */
  def getFileType(s: String): FileType =
    FileType
      .withNameInsensitiveOption(s)
      .getOrElse(FileType.GenericData)

  /**
    * Convert a file extension to a Pennsieve FileType.
    */
  def getFileTypeFromExtension(s: String): FileType = {
    val withDot = if (s.startsWith(".")) s else s".$s"
    FileExtensions.fileTypeMap.getOrElse(withDot, FileType.GenericData)
  }

  /**
    * Get the Pennsieve package type for a file type.
    *
    * The frontend uses the package type to display icons in the file browser.
    */
  def getPackageType(fileType: FileType): PackageType =
    FileTypeInfo.get(fileType).packageType

  def getIcon(fileType: FileType): Icon =
    FileTypeInfo
      .get(fileType)
      .icon

  /**
    *
    * @param athenaDownloads   : An array of DatasetDownload originating from Athena that may contains info already in the
    *                          database
    * @param databaseDownloads : An array of DatasetDownload originating from discover database
    * @return : An Array of DatasetDownload contains the downloads from both origins, once deduplicated
    */
  // Deduplication:
  // An Athena Download is considered a duplicate of a Database Download if at least one of those is true:
  // - Both Downloads have the same request ID
  // - Both Downloads are for the same dataset, same version within 2 seconds of each other
  def cleanAthenaDownloads(
    athenaDownloads: List[DatasetDownload],
    databaseDownloads: List[DatasetDownload]
  ): List[DatasetDownload] = {
    athenaDownloads.filter { athenaDL =>
      databaseDownloads.collectFirst {
        case databaseDL
            if (databaseDL.requestId == athenaDL.requestId) && (databaseDL.requestId.isDefined) ||
              ((databaseDL.datasetId == athenaDL.datasetId) && (databaseDL.version == athenaDL.version) && (
                athenaDL.downloadedAt
                  .until(databaseDL.downloadedAt, ChronoUnit.SECONDS)
                  .abs < 2
              )) =>
          databaseDL
      }.isEmpty
    }
  }

  def getOrCreateDoi(
    ports: Ports,
    organizationId: Int,
    datasetId: Int
  )(implicit
    ec: ExecutionContext,
    logContext: DiscoverLogContext
  ): Future[DoiDTO] = {
    val token = Authenticator.generateServiceToken(
      ports.jwt,
      organizationId = organizationId,
      datasetId = datasetId
    )
    val headers = List(Authorization(OAuth2BearerToken(token.value)))

    for {
      latestDoi <- ports.doiClient
        .getLatestDoi(organizationId, datasetId, headers)
        .recoverWith {
          case NoDoiException => {
            // no DOI exists for the dataset, so create a new one
            ports.log.info("creating new DOI: no existing DOI found")
            ports.doiClient.createDraftDoi(organizationId, datasetId, headers)
          }
        }
      isDuplicateDoi <- ports.db.run(
        PublicDatasetVersionsMapper.isDuplicateDoi(latestDoi.doi)
      )
      isFindable = latestDoi.state.contains(DoiState.Findable)
      validDoi <- if (isFindable || isDuplicateDoi) {
        // create a new draft DOI if the latest DOI is Findable, or if the latest DOI is already associated with a dataset version
        ports.log.info(
          s"creating new DOI: existing DOI ${latestDoi.doi} is not usable (isFindable: ${isFindable}, isDuplicateDoi: ${isDuplicateDoi})"
        )
        ports.doiClient.createDraftDoi(organizationId, datasetId, headers)
      } else Future.successful(latestDoi)
    } yield validDoi
  }

  def deleteAssets(
    lambdaClient: LambdaClient,
    s3KeyPrefix: String,
    publishBucket: String,
    embargoBucket: String,
    migrated: Boolean
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    lambdaClient.runS3Clean(
      s3KeyPrefix,
      publishBucket,
      embargoBucket,
      S3CleanupStage.Unpublish,
      migrated
    )
  }

  def deleteAssetsMulti(
    lambdaClient: LambdaClient,
    s3KeyPrefix: String,
    buckets: Set[S3Bucket],
    migrated: Boolean
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[Iterator[InvokeResponse]] = {
    val atMostTwoAtATime = buckets.grouped(2)
    Future.sequence(
      atMostTwoAtATime
        .map(_.toList match {
          case List(S3Bucket(b1), S3Bucket(b2)) =>
            deleteAssets(lambdaClient, s3KeyPrefix, b1, b2, migrated)
          case List(S3Bucket(b)) =>
            deleteAssets(lambdaClient, s3KeyPrefix, b, b, migrated)
          case _ =>
            throw new AssertionError(
              s"${atMostTwoAtATime} shouldn't produce lists with more than two elements!"
            )
        })
    )
  }

}
