// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.alpakka.s3.ListBucketResultContents
import akka.stream.alpakka.s3.scaladsl.S3
import akka.actor.ActorSystem
import akka.util.ByteString

import com.blackfynn.models.{ FileManifest, FileType }
import com.blackfynn.discover.models._
import com.blackfynn.test.AwaitableImplicits
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import java.nio.file.{ Files, Path, Paths }
import java.util.UUID
import java.util.Comparator
import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt
import scala.sys.process._

import java.nio.file.{ Files, Path, Paths }
import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.mutable
import com.blackfynn.test.AwaitableImplicits
import com.blackfynn.discover.downloads.ZipStream._

class MockS3StreamClient extends S3StreamClient {

  def getPresignedUrlForFile(bucket: S3Bucket, key: S3Key.File): String =
    s"https://$bucket.s3.amazonaws.com/$key"

  private var nextResponse: Option[List[TestFile]] = None

  def withNextResponse(testFiles: List[TestFile]) = {
    nextResponse = Some(testFiles)
  }

  def datasetFilesSource(
    version: PublicDatasetVersion,
    zipPrefix: String
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[ZipSource, NotUsed] = {
    val testFiles = nextResponse match {
      case Some(testFiles) => testFiles
      case None => throw new Exception("use withNextResponse to set test data")
    }
    testFiles.foreach(_.generate)
    val source = Source(testFiles.map(_.zipSource))
    nextResponse = None
    source
  }

  val sampleMetadata =
    """
{
  "name" : "Test Dataset",
  "pennsieveDatasetId": "1",
  "version": "1",
  "description" : "Lorem ipsum",
  "creator" : { "first_name": "Blaise", "last_name": "Pascal", "orcid": "0000-0009-1234-5678"},
  "sourceOrganization" : "1",
  "contributors" : [  { "first_name": "Isaac", "last_name": "Newton"}, { "first_name": "Albert", "last_name": "Einstein"}],
  "datePublished": "2019-06-05",
  "license": "MIT",
  "@id": "10.21397/jlt1-xdqn",
  "@type":"Dataset",
  "schemaVersion": "http://schema.org/version/3.7/",
  "keywords" : [
    "neuro",
    "neuron"
  ],
  "date" : "2019-06-06",
  "rights" : "MIT",
  "identifier" : "10.21397/jlt1-xdqn",
  "publisher" : "University of Pennsylvania",
  "@context" : "http://purl.org/dc/terms",
  "pennsieveSchemaVersion" : "3.0",
  "files" : [
    {
      "path" : "files/brain.dcm",
      "size" : 15010,
      "fileType" : "DICOM",
      "sourcePackageId" : "N:package:1"
    }
]}"""

  def datasetMetadataSource(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[(Source[ByteString, NotUsed], Long)] =
    Future.successful(
      (Source.single(ByteString(sampleMetadata)), sampleMetadata.length)
    )

  val revisions: mutable.ArrayBuffer[
    (PublicDataset, PublicDatasetVersion, List[PublicContributor], Revision)
  ] =
    mutable.ArrayBuffer.empty

  def writeDatasetFilesList(
    version: PublicDatasetVersion,
    files: List[PublishedFile]
  )(implicit
    system: ActorSystem,
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[S3Key.File] = {
    Future(S3Key.File("versions.json"))
  }

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
    revisions += ((dataset, version, contributors, revision))
    Future.successful(
      NewFiles(
        manifest = FileManifest(
          path = s"revisions/${revision.revision}/manifest.json",
          size = 100,
          fileType = FileType.Json
        ),
        readme = FileManifest(
          path = s"revisions/${revision.revision}/readme.md",
          size = 100,
          fileType = FileType.Markdown
        ),
        banner = FileManifest(
          path = s"revisions/${revision.revision}/banner.jpg",
          size = 100,
          fileType = FileType.JPEG
        )
      )
    )
  }

  val sampleReadme = "This is a readme"
  val revisedReadme = "This is a revised readme"

  def readDatasetReadme(
    version: PublicDatasetVersion,
    revision: Option[Revision]
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Readme] =
    Future.successful(
      Readme(revision.map(_ => revisedReadme).getOrElse(sampleReadme))
    )

  val publishResults: mutable.Map[S3Key.Dataset, PublishJobOutput] =
    mutable.Map.empty

  def withNextPublishResult(key: S3Key.Dataset, result: PublishJobOutput) =
    publishResults += key -> result

  def readPublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[PublishJobOutput] = Future(publishResults(version.s3Key))

  def deletePublishJobOutput(
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[Unit] = Future(publishResults -= version.s3Key)

  def datasetRecordSource(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Source[Record, NotUsed] =
    Source.single(
      Record("patient", dataset, version, Map("DOB" -> "06/05/2001"))
    )

  def clear(): Unit = {
    revisions.clear()
  }
}

case class TestFile(
  numBytes: Int,
  tempDir: Path,
  sourcePath: String,
  destPath: String // path in the zip file
) extends AwaitableImplicits {

  // create the source file, filled with random bytes
  def generate(implicit materializer: ActorMaterializer): Unit = {
    Source
      .repeat(NotUsed)
      .take(numBytes)
      .map(_ => ByteString((scala.util.Random.nextInt(256) - 128).toByte))
      .runWith(FileIO.toPath(tempDir.resolve(sourcePath)))
      .awaitFinite()
  }

  // raw input source file as akka Source
  def sourceFile: Source[ByteString, NotUsed] =
    FileIO
      .fromPath(tempDir.resolve(sourcePath))
      .mapMaterializedValue(_ => NotUsed)

  // outfile in unzipped archive as akka Source
  def outputFile: Source[ByteString, NotUsed] =
    FileIO
      .fromPath(tempDir.resolve(destPath))
      .mapMaterializedValue(_ => NotUsed)

  def zipSource: ZipSource = (destPath, sourceFile)

}

object TestFile extends AwaitableImplicits {

  /**
    * Check that the bytes of every extracted file are identical to the original
    */
  def sourceAndDestAreEqual(
    testFiles: List[TestFile]
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Boolean =
    Future
      .sequence(
        testFiles.map(
          testFile =>
            testFile.sourceFile
              .flatMapConcat(Source(_))
              .zip(testFile.outputFile.flatMapConcat(Source(_)))
              .map {
                case (byte1: Byte, byte2: Byte) =>
                  byte1 == byte2
              }
              .runWith(Sink.fold(true)(_ && _))
        )
      )
      .awaitFinite()
      .forall(_ == true)
}
