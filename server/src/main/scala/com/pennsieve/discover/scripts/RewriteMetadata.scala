// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.scripts

import akka.{ Done, NotUsed }
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.scaladsl.S3
import com.pennsieve.models.{ License, PublishStatus }
import com.pennsieve.discover._
import com.pennsieve.discover.db._
import com.pennsieve.discover.models.PublicDatasetVersion
import com.pennsieve.models.{ FileExtensions, FileType }
import com.pennsieve.discover.db.profile.api._
import com.typesafe.scalalogging.StrictLogging
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }
import io.circe.{ Decoder, Encoder }
import io.scalaland.chimney.dsl._
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.DecodingFailure

import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }
import java.time.LocalDate

object RewriteMetadata extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val dryRun: Boolean = args.headOption match {
      case Some("false") => false
      case _ => true
    }

    if (dryRun) println("*** DRY RUN ***")

    val config: Config = Config.load

    implicit val system: ActorSystem = ActorSystem("discover-service")
    implicit val executionContext: ExecutionContext = system.dispatcher

    implicit val ports: Ports = Ports(config)

    try {

      val query = PublicDatasetVersionsMapper
        .filter(_.status === (PublishStatus.PublishSucceeded: PublishStatus))
        .result
        .map(_.toList)
        .map(Source(_))

      val end = Source
        .futureSource(ports.db.run(query))
        .mapAsync(1)(version => {
          println(s"\nDataset ${version.datasetId}/${version.version}")

          (for {
            bannerManifest <- S3
              .getObjectMetadata(
                config.s3.publishBucket.value,
                (version.s3Key / "banner.jpg").value
              )
              .runWith(Sink.head)
              .flatMap {
                case Some(m) =>
                  Future.successful(
                    LocalFileManifest("banner.jpg", m.getContentLength, "JPEG")
                  )
                case None => Future.failed(new Exception("Banner not found"))
              }

            readmeManifest <- S3
              .getObjectMetadata(
                config.s3.publishBucket.value,
                (version.s3Key / "readme.md").value
              )
              .runWith(Sink.head)
              .flatMap {
                case Some(m) =>
                  Future.successful(
                    LocalFileManifest(
                      "readme.md",
                      m.getContentLength,
                      "Markdown"
                    )
                  )
                case None => Future.failed(new Exception("Readme not found"))
              }

            graphManifests <- S3
              .listBucket(
                config.s3.publishBucket.value,
                Some((version.s3Key / "graph").value)
              )
              .map(s3Object => {
                val path = s3Object.key.stripPrefix(version.s3Key.value)
                val size = s3Object.size
                val (_, extension) = utilities.splitFileName(path)
                val fileType = utilities.getFileType(extension)

                LocalFileManifest(path, size, fileType.toString)
              })
              .runWith(Sink.seq)
              .map(_.toList)

            updatedMetadata <- buildUpdatedMetadata(
              ports,
              version,
              bannerManifest :: readmeManifest :: graphManifests
            )

            // If the dataset already has a readme entry, then it was created
            // by the updated publish job
            alreadyUpdated <- ports.s3StreamClient
              .readDatasetMetadata(version)
              .map(_.files.find(_.path == "readme.md").isDefined)

            _ <- if (alreadyUpdated) {
              println("SKIPPING - ALREADY HAS README")
              Future.successful(())
            } else if (dryRun) {
              Future.successful(())
            } else {
              print("Writing updated metadata...")

              val query = for {
                _ <- DBIO.from(
                  Source
                    .single(ByteString(updatedMetadata.asJson.toString))
                    .runWith(
                      S3.multipartUpload(
                        config.s3.publishBucket.value,
                        (version.s3Key / "metadata.json").value
                      )
                    )
                )

                totalSize <- DBIO.from(computeTotalSize(config, version))
                _ = println(s"Updating size from ${version.size} to $totalSize")

                _ <- PublicDatasetVersionsMapper
                  .filter(_.datasetId === version.datasetId)
                  .filter(_.version === version.version)
                  .map(_.size)
                  .update(totalSize)

              } yield ()

              ports.db.run(query.transactionally)
            }
          } yield ()).recover {
            case e: DecodingFailure => {
              println(
                s"Cannot update ${version.datasetId}/${version.version}",
                e
              )
            }

            case e: S3Exception => {
              println(
                s"Cannot update ${version.datasetId}/${version.version}",
                e
              )
            }

            case e: Throwable => {
              println(
                s"Cannot update ${version.datasetId}/${version.version}",
                e
              )
              if (!dryRun) throw e
            }
          }
        })
        .runWith(Sink.seq)

      Await.result(end, 10.minutes)
      logger.info("Done")

    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    } finally {
      logger.info("Shutting down actor system...")
      ports.db.close()
      Await.result(system.terminate(), 5.seconds)
    }
  }
  def buildUpdatedMetadata(
    ports: Ports,
    version: PublicDatasetVersion,
    toAdd: List[LocalFileManifest]
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[DatasetMetadata] =
    for {
      // Download metadata JSON
      result <- ports.s3StreamClient
        .datasetMetadataSource(version)
      (source, _) = result
      content <- source
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .map(_.utf8String)
      metadata <- decode[DatasetMetadata](content)
        .fold(Future.failed, Future.successful)

      originalMetadata = metadata
        .into[DatasetMetadata]
        .withFieldComputed(
          _.files,
          _ =>
            metadata.files.map(
              _.into[LocalFileManifest]
                .withFieldComputed(_.fileType, _.toString())
                .transform
            )
        )
        .transform

      metadataFilename = "metadata.json"
      metadataManifest = LocalFileManifest(metadataFilename, 0, "Json")

      unsizedMetadata = metadata
        .copy(files = originalMetadata.files ++ (metadataManifest :: toAdd))

      metadataSize = sizeCountingOwnSize(
        unsizedMetadata.asJson.toString.getBytes("utf-8").length - 1
      )

      updatedManifests = metadataManifest.copy(size = metadataSize) :: toAdd

      _ = updatedManifests.foreach(m => println(s" - $m"))

    } yield
      unsizedMetadata.copy(files = (originalMetadata.files ++ updatedManifests))

  /**
    * Compute the total storage size of the dataset by scanning the S3 objects
    * under the publish key.
    */
  def computeTotalSize(
    config: Config,
    version: PublicDatasetVersion
  )(implicit
    executionContext: ExecutionContext,
    materializer: Materializer
  ): Future[Long] =
    S3.listBucket(
        config.s3.publishBucket.value,
        Some(StringUtils.appendIfMissing(version.s3Key.value, "/"))
      )
      .map(_.size)
      .runWith(Sink.fold(0: Long)(_ + _))

  def sizeCountingOwnSize(x: Int): Int = {
    val guess = x + strlen(x)
    if (strlen(guess) > strlen(x))
      guess + 1
    else
      guess
  }
  def strlen(x: Int): Int = x.toString.length

}

case class DatasetMetadata(
  pennsieveDatasetId: Int,
  version: Int,
  name: String,
  description: String,
  creator: String,
  contributors: List[String],
  sourceOrganization: String,
  keywords: List[String],
  datePublished: LocalDate,
  license: Option[License],
  `@id`: String, // DOI
  publisher: String = "University of Pennsylvania",
  `@context`: String = "http://schema.org/",
  `@type`: String = "Dataset",
  schemaVersion: String = "http://schema.org/version/3.7/",
  pennsieveSchemaVersion: Int = 1,
  files: List[LocalFileManifest] = List.empty
)

object DatasetMetadata {
  implicit val encoder: Encoder[DatasetMetadata] =
    deriveEncoder[DatasetMetadata]
  implicit val decoder: Decoder[DatasetMetadata] =
    deriveDecoder[DatasetMetadata]
}

case class LocalFileManifest(path: String, size: Long, fileType: String)

object LocalFileManifest {
  implicit val encoder: Encoder[LocalFileManifest] =
    deriveEncoder[LocalFileManifest]
  implicit val decoder: Decoder[LocalFileManifest] =
    deriveDecoder[LocalFileManifest]
}

object utilities {
  def getFileType: String => FileType =
    FileExtensions.fileTypeMap getOrElse (_, FileType.GenericData)

  def splitFileName(fileName: String): (String, String) = {
    // if one exists, return the first extension from the map that the file name ends with
    // otherwise return no extension (the empty string)
    val extensions = FileExtensions.fileTypeMap.keys
      .filter(extension => fileName.toLowerCase.endsWith(extension))

    val extension = extensions.foldLeft("") { (current, next) =>
      if (next.length() > current.length()) next
      else current
    }

    // strip the extension and directories off the file name
    val baseName = FilenameUtils.getName(fileName.dropRight(extension.length))

    (baseName, extension)
  }

}
