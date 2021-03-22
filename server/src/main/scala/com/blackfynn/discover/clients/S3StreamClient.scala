// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.ByteRange
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.alpakka.s3.ListBucketResultContents
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.util.ByteString
import com.pennsieve.discover.{ utils, S3Exception }
import com.pennsieve.discover.downloads.ZipStream._
import com.pennsieve.discover.models._
import com.pennsieve.models._
import com.pennsieve.discover.utils.joinPath
import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FilenameUtils
import squants.information.Information
import squants.information.InformationConversions._

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import io.circe.syntax._
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.{ Decoder, Encoder, Printer }
import io.circe.parser.decode
import io.scalaland.chimney.dsl._
import java.nio.charset.StandardCharsets
import java.time.Duration

import com.pennsieve.discover.models.Revision
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest

trait S3StreamClient {

  def datasetFilesSource(
    version: PublicDatasetVersion,
    zipPrefix: String // folder under which to place files in the zip archive
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[ZipSource, NotUsed]

  def datasetMetadataSource(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[(Source[ByteString, NotUsed], Long)]

  def datasetRecordSource(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[Record, NotUsed]

  /**
    * Read the dataset readme file from S3. This can either be at the root of
    * the dataset version, or under the latest revision in the revisions
    * directory.
    */
  def readDatasetReadme(
    version: PublicDatasetVersion,
    revision: Option[Revision]
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Readme]

  def readPublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[PublishJobOutput]

  def deletePublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Unit]

  /**
    * Read the dataset metadata file from S3
    */
  def readDatasetMetadata(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[DatasetMetadata] =
    for {
      (source, _) <- datasetMetadataSource(version)

      content <- source
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .map(_.utf8String)

      output <- decode[DatasetMetadata](content)
        .fold(Future.failed, Future.successful)
    } yield output

  def writeDatasetRevisionMetadata(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    revision: Revision,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    bannerPresignedUrl: Uri,
    readmePresignedUrl: Uri
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[NewFiles]

  case class NewFiles(
    banner: FileManifest,
    readme: FileManifest,
    manifest: FileManifest
  ) {
    def asList = List(banner, readme, manifest)
  }

  def getPresignedUrlForFile(s3Bucket: S3Bucket, key: S3Key.File): String
}

class AlpakkaS3StreamClient(
  region: Region,
  frontendBucket: S3Bucket,
  assetsKeyPrefix: String,
  chunkSize: Information = 20.megabytes
) extends S3StreamClient
    with StrictLogging {

  private val MANIFEST_FILE = "manifest.json"
  private val README_FILE = "readme.md"
  private val BANNER = "banner"
  private val README = "readme"

  private val chunkSizeBytes: Long = chunkSize.toBytes.toLong

  lazy val s3Presigner = S3Presigner.builder.region(region).build

  /**
    * Assumed locations of items in the publish bucket.
    *
    * TODO: share/communicate these locations from publish job outputs
    */
  private def metadataKey(version: PublicDatasetVersion): S3Key.File = {
    version.schemaVersion match {
      case PennsieveSchemaVersion.`1.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`2.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`3.0` => version.s3Key / "metadata.json"
      case PennsieveSchemaVersion.`4.0` => version.s3Key / MANIFEST_FILE
    }
  }

  private def readmeKey(
    version: PublicDatasetVersion,
    revision: Option[Revision]
  ): S3Key.File =
    revision.map(_.s3Key / README_FILE).getOrElse(version.s3Key / README_FILE)

  private def outputKey(version: PublicDatasetVersion): S3Key.File =
    version.s3Key / "outputs.json"

  private def graphSchemaKey(version: PublicDatasetVersion): S3Key.File = {
    version.schemaVersion match {
      case PennsieveSchemaVersion.`1.0` => version.s3Key / "graph/schema.json"
      case PennsieveSchemaVersion.`2.0` => version.s3Key / "graph/schema.json"
      case PennsieveSchemaVersion.`3.0` => version.s3Key / "graph/schema.json"
      case PennsieveSchemaVersion.`4.0` =>
        version.s3Key / "metadata/schema.json"
    }
  }

  /**
    * State machine used to unfold a download from S3.
    */
  private sealed trait DownloadState
  private case object Starting extends DownloadState
  private case class InProgress(nextByte: Long) extends DownloadState
  private case object Finished extends DownloadState

  /**
    * Source that streams all S3 objects nested under the given key into a ZipStream.
    */
  def datasetFilesSource(
    version: PublicDatasetVersion,
    zipPrefix: String // folder under which to place files in the zip archive
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[ZipSource, NotUsed] = {
    logger.info(s"Listing s3://${version.s3Bucket}")
    S3.listBucket(version.s3Bucket.value, Some(version.s3Key.value))
      .map { s3Object: ListBucketResultContents =>
        logger.info(s"Downloading s3://${version.s3Bucket}/${s3Object.key}")

        val zipEntryName =
          joinPath(zipPrefix, s3Object.key.stripPrefix(version.s3Key.value))

        // This is a convoluted way to build a source for each file without
        // using mapAsync, which greedily starts to download files without
        // waiting for a pull. This leads to "Substream source cannot be
        // materialized more than once" errors when a large file is being
        // downloaded and the next download times out waiting to be pulled
        // downstream. unfoldAsync only evalates the source on pull.
        val byteSource = Source
          .unfoldAsync[DownloadState, Source[ByteString, NotUsed]](Starting) {
            case Starting => downloadRange(s3Object, 0)
            case InProgress(nextByte) =>
              downloadRange(s3Object, nextByte)
            case Finished => Future.successful(None)
          }
          .flatMapConcat(s => s)
        (zipEntryName, byteSource)
      }
  }

  /**
    * Download a chunk of data from a file in S3. Returns the next state
    * in the download state machine.
    */
  private def downloadRange(
    s3Object: ListBucketResultContents,
    start: Long
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Option[(DownloadState, Source[ByteString, NotUsed])]] = {

    val (byteRange, nextState) =
      if ((start + chunkSizeBytes) >= s3Object.size)
        (ByteRange(start, s3Object.size), Finished)
      else
        (
          ByteRange(start, start + chunkSizeBytes - 1),
          InProgress(start + chunkSizeBytes)
        )

    S3.download(s3Object.bucketName, s3Object.key, Some(byteRange))
      .recoverWithRetries(attempts = 2, {
        case e: TcpIdleTimeoutException =>
          logger.error("TCP Idle Timeout", e)
          S3.download(s3Object.bucketName, s3Object.key, Some(byteRange))
      })
      .runWith(Sink.head)
      .flatMap {
        case Some((source, metadata)) =>
          Future.successful(Some((nextState, source)))
        case None =>
          Future.failed(
            S3Exception(S3Bucket(s3Object.bucketName), S3Key.File(s3Object.key))
          )
      }
  }

  /**
    * Stream the metadata.json file from S3 for a dataset.
    */
  def datasetMetadataSource(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[(Source[ByteString, NotUsed], Long)] =
    s3FileSource(version.s3Bucket, metadataKey(version))

  /**
    * Write all metadata files for a revision to S3.
    *
    * Also write the banner and readme to the frontend bucket.
    */
  def writeDatasetRevisionMetadata(
    dataset: PublicDataset,
    version: PublicDatasetVersion,
    contributors: List[PublicContributor],
    revision: Revision,
    collections: List[PublicCollection],
    externalPublications: List[PublicExternalPublication],
    bannerPresignedUrl: Uri,
    readmePresignedUrl: Uri
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[NewFiles] = {

    val metadata = DatasetMetadataV4_0(
      pennsieveDatasetId = dataset.id,
      version = version.version,
      revision = Some(revision.revision),
      name = dataset.name,
      description = version.description,
      creator = PublishedContributor(
        first_name = dataset.ownerFirstName,
        last_name = dataset.ownerLastName,
        orcid = Some(dataset.ownerOrcid)
      ),
      contributors = contributors.map(
        c =>
          PublishedContributor(
            first_name = c.firstName,
            last_name = c.lastName,
            orcid = c.orcid
          )
      ),
      collections =
        Some(collections.map(c => PublishedCollection(name = c.name))),
      relatedPublications = Some(
        externalPublications
          .map(
            p =>
              PublishedExternalPublication(Doi(p.doi), Some(p.relationshipType))
          )
      ),
      sourceOrganization = dataset.sourceOrganizationName,
      keywords = dataset.tags,
      datePublished = version.createdAt.toLocalDate,
      license = Some(dataset.license),
      `@id` = version.doi
    )

    val key = S3Key.Revision(dataset.id, version.version, revision.revision)

    // Remove the empty file field
    implicit val encoder: Encoder[DatasetMetadataV4_0] =
      deriveEncoder[DatasetMetadataV4_0].mapJson(_.mapObject(_.remove("files")))

    // Remove null fields when printing
    val bytes = ByteString(
      Printer.spaces2.copy(dropNullValues = true).pretty(metadata.asJson)
    )

    for {
      bannerManifest <- copyPresignedUrlToRevision(
        bannerPresignedUrl,
        key / newNameSameExtension(bannerPresignedUrl, BANNER),
        version
      )
      readmeManifest <- copyPresignedUrlToRevision(
        readmePresignedUrl,
        key / newNameSameExtension(readmePresignedUrl, README),
        version
      )

      manifestManifest <- Source
        .single(bytes)
        .runWith(
          S3.multipartUpload(
            version.s3Bucket.value,
            (key / MANIFEST_FILE).toString
          )
        )
        .map(
          result =>
            FileManifest(
              path = (key / MANIFEST_FILE)
                .removeVersionPrefix(version.s3Key)
                .toString,
              size = bytes.length,
              fileType = FileType.Json
            )
        )

      _ <- copyPresignedUrlToFrontendBucket(
        bannerPresignedUrl,
        key / newNameSameExtension(bannerPresignedUrl, BANNER)
      )
      _ <- copyPresignedUrlToFrontendBucket(
        readmePresignedUrl,
        key / newNameSameExtension(readmePresignedUrl, README)
      )

    } yield
      NewFiles(
        manifest = manifestManifest,
        readme = readmeManifest,
        banner = bannerManifest
      )
  }

  private def newNameSameExtension(
    presignedUrl: Uri,
    newName: String
  ): String = {
    val extension = FilenameUtils.getExtension(presignedUrl.path.toString)
    s"$newName.$extension"
  }

  private def copyPresignedUrlToRevision(
    presignedUrl: Uri,
    key: S3Key.File,
    version: PublicDatasetVersion
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[FileManifest] = {
    for {
      (contentType, source) <- streamPresignedUrl(presignedUrl)

      _ <- source.runWith(
        S3.multipartUpload(
          version.s3Bucket.value,
          key.toString,
          contentType = contentType
        )
      )
      size <- getObjectSize(version.s3Bucket, key)
    } yield
      FileManifest(
        path = key.removeVersionPrefix(version.s3Key).toString,
        size = size,
        fileType = utils.getFileTypeFromExtension(
          FilenameUtils.getExtension(key.toString)
        )
      )
  }

  private def copyPresignedUrlToFrontendBucket(
    presignedUrl: Uri,
    key: S3Key.File
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[String] = {
    for {
      (contentType, source) <- streamPresignedUrl(presignedUrl)

      response <- source.runWith(
        S3.multipartUpload(
          frontendBucket.value,
          joinPath(assetsKeyPrefix, key.toString),
          contentType = contentType
        )
      )
    } yield response.key
  }

  private def streamPresignedUrl(
    presignedUrl: Uri
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[(ContentType, Source[ByteString, NotUsed])] = {
    for {
      resp <- Http().singleRequest(
        HttpRequest(uri = presignedUrl, method = HttpMethods.GET)
      )
      body <- if (resp.status.isSuccess())
        Future.successful(
          Right(
            (
              resp.entity.contentType,
              resp.entity.dataBytes.mapMaterializedValue(_ => NotUsed)
            )
          )
        )
      else
        resp.entity
          .toStrict(3.seconds)
          .map(body => Left(HttpError(resp.status, body.data.utf8String)))

      bytes <- body.fold(Future.failed(_), Future.successful(_))

    } yield bytes
  }

  private def getObjectSize(
    bucket: S3Bucket,
    key: S3Key.File
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Long] =
    for {
      maybeMetadata <- S3
        .getObjectMetadata(bucket.value, key.toString)
        .runWith(Sink.head)
      size <- maybeMetadata
        .map(metadata => Future.successful(metadata.contentLength))
        .getOrElse(Future.failed(S3Exception(bucket, key)))
    } yield size

  /**
    * Stream the readme file from S3 for a dataset.
    */
  def readDatasetReadme(
    version: PublicDatasetVersion,
    revision: Option[Revision]
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Readme] =
    for {
      (source, _) <- s3FileSource(
        version.s3Bucket,
        readmeKey(version, revision)
      )

      content <- source
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .map(_.utf8String)
    } yield Readme(content)

  /**
    * Read the outputs.json file from a successful publish job.
    */
  def readPublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[PublishJobOutput] =
    for {
      (source, _) <- s3FileSource(version.s3Bucket, outputKey(version))

      content <- source
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .map(_.utf8String)

      output <- decode[PublishJobOutput](content)
        .fold(Future.failed, Future.successful)
    } yield output

  def s3FileSource(
    bucket: S3Bucket,
    fileKey: S3Key.File
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[(Source[ByteString, NotUsed], Long)] =
    S3.download(bucket.value, fileKey.value)
      .runWith(Sink.head)
      .flatMap {
        case Some((source, content)) =>
          Future.successful((source, content.getContentLength))
        case None =>
          Future.failed(S3Exception(bucket, fileKey))
      }

  /**
    * Delete the outputs.json file so that it does not appear in the published
    * dataset.
    */
  def deletePublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Unit] =
    S3.deleteObject(version.s3Bucket.value, outputKey(version).value)
      .runWith(Sink.head)
      .map(_ => ())

  /**
    * Source containing all the records in a dataset.
    *
    * This function introspects the metadata and schema of a published dataset
    * to build the concatenated stream.
    */
  def datasetRecordSource(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[Record, NotUsed] =
    datasetModelsSource(version)
      .flatMapConcat(
        model =>
          s3CsvSource(version.s3Bucket, model.csvKey(version)).map(
            m =>
              Record(
                model = model.name,
                datasetId = dataset.id,
                version = version.version,
                organizationName = dataset.sourceOrganizationName,
                properties = m
              )
          )
      )

  def getPresignedUrlForFile(bucket: S3Bucket, key: S3Key.File): String = {
    val objectRequest = GetObjectRequest.builder
      .bucket(bucket.value)
      .key(key.value)
      .build
    val presignedRequest = GetObjectPresignRequest.builder
      .signatureDuration(Duration.ofNanos(60.minutes.toNanos))
      .getObjectRequest(objectRequest)
      .build
    s3Presigner.presignGetObject(presignedRequest).url.toString
  }

  case class ModelSchema(name: String, file: String) {
    def csvKey(version: PublicDatasetVersion): S3Key.File = {
      version.schemaVersion match {
        case PennsieveSchemaVersion.`1.0` => version.s3Key / s"graph/$file"
        case PennsieveSchemaVersion.`2.0` => version.s3Key / s"graph/$file"
        case PennsieveSchemaVersion.`3.0` => version.s3Key / s"graph/$file"
        case PennsieveSchemaVersion.`4.0` => version.s3Key / s"metadata/$file"
      }
    }

  }

  object ModelSchema {
    implicit val encoder: Encoder[ModelSchema] = deriveEncoder[ModelSchema]
    implicit val decoder: Decoder[ModelSchema] = deriveDecoder[ModelSchema]
  }

  case class GraphSchema(models: List[ModelSchema])

  object GraphSchema {
    implicit val encoder: Encoder[GraphSchema] = deriveEncoder[GraphSchema]
    implicit val decoder: Decoder[GraphSchema] = deriveDecoder[GraphSchema]
  }

  /**
    * Parse the graph schema of a dataset an generate a stream of
    * models defined in the schema.
    */
  private def datasetModelsSource(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[ModelSchema, NotUsed] = {
    val graphSchema = Source
      .fromFutureSource(
        s3FileSource(version.s3Bucket, graphSchemaKey(version)).map(_._1)
      )
      .runWith(Sink.fold(ByteString.empty)(_ ++ _))
      .map(_.utf8String)
      .flatMap(
        decode[GraphSchema](_)
          .fold(Future.failed, Future.successful)
      )
      // If the dataset does not have graph data, getting the graph schema
      // will fail with an S3Exception.
      .recoverWith {
        case e: S3Exception => Future.successful(GraphSchema(List.empty))
      }

    Source
      .fromFuture(graphSchema)
      .mapConcat(_.models)
  }

  private def s3CsvSource(
    bucket: S3Bucket,
    fileKey: S3Key.File
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[Map[String, String], NotUsed] =
    Source
      .fromFutureSource(s3FileSource(bucket, fileKey).map(_._1))
      .via(CsvParsing.lineScanner())
      .via(CsvToMap.toMapAsStrings(StandardCharsets.UTF_8))
      .mapMaterializedValue(_ => NotUsed)
}
