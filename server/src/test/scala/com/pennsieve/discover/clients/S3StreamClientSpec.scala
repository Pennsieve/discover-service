// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.testkit.scaladsl.TestSink
import akka.util.ByteString
import com.pennsieve.discover.{
  DockerS3Service,
  ExternalPublishBucketConfiguration,
  S3Exception
}
import com.pennsieve.discover.models._
import com.pennsieve.models._
import com.pennsieve.test.AwaitableImplicits
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import com.whisk.docker.scalatest.DockerTestKit
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.spotify.docker.client.exceptions.DockerException
import com.spotify.docker.client.DefaultDockerClient
import io.circe.syntax._
import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto.deriveDecoder
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues._
import software.amazon.awssdk.arns.Arn
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.model.{
  CreateBucketRequest,
  GetObjectRequest,
  PutObjectRequest
}
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.services.sts.StsClient
import squants.information.Information
import squants.information.InformationConversions._

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.nio.file.{ Path, Paths }
import java.time.OffsetDateTime
import scala.compat.java8.DurationConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

class S3StreamClientSpec
    extends AnyWordSpec
    with Matchers
    with AwaitableImplicits
    with ScalaFutures
    with DockerS3Service
    with DockerTestKit {

  // alpakka-s3 v1.0 can only be configured via Typesafe config passed to the
  // actor system, or as S3Settings that are attached to every graph
  lazy val config = ConfigFactory
    .load()
    .withValue(
      "alpakka.s3.endpoint-url",
      ConfigValueFactory.fromAnyRef(s3Endpoint)
    )
    .withValue(
      "alpakka.s3.aws.credentials.provider",
      ConfigValueFactory.fromAnyRef("static")
    )
    .withValue(
      "alpakka.s3.aws.credentials.access-key-id",
      ConfigValueFactory.fromAnyRef(accessKey)
    )
    .withValue(
      "alpakka.s3.aws.credentials.secret-access-key",
      ConfigValueFactory.fromAnyRef(secretKey)
    )
    .withValue("alpakka.s3.access-style", ConfigValueFactory.fromAnyRef("path"))

  implicit lazy private val system: ActorSystem =
    ActorSystem("discover-service", config)
  implicit lazy private val executionContext: ExecutionContext =
    system.dispatcher

  override implicit val dockerFactory: DockerFactory =
    try new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
    catch {
      case _: DockerException =>
        throw new DockerException("Docker may not be running")
    }

  lazy val s3Presigner: S3Presigner = S3Presigner
    .builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(
      StaticCredentialsProvider
        .create(AwsBasicCredentials.create(accessKey, secretKey))
    )
    .endpointOverride(new URI(s3Endpoint))
    .build()

  lazy val sharedHttpClient = UrlConnectionHttpClient.builder().build()

  lazy val s3Client: S3Client = S3Client
    .builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(
      StaticCredentialsProvider
        .create(AwsBasicCredentials.create(accessKey, secretKey))
    )
    .httpClient(sharedHttpClient)
    .endpointOverride(new URI(s3Endpoint))
    .build()

  lazy val stsClient: StsClient = StsClient
    .builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(
      StaticCredentialsProvider
        .create(AwsBasicCredentials.create(accessKey, secretKey))
    )
    .endpointOverride(new URI(s3Endpoint))
    .httpClient(sharedHttpClient)
    .build()

  /**
    * Create a streaming client for testing, and the required S3 buckets.
    */
  def createClient(
    chunkSize: Information = 20.megabytes,
    externalPublishBucketConfig: Option[ExternalPublishBucketConfiguration] =
      None
  ): (AlpakkaS3StreamClient, String, String) = {

    val publishBucket = s"publish-bucket-${UUID.randomUUID()}"
    createBucket(publishBucket)

    val frontendBucket = s"frontend-bucket-${UUID.randomUUID()}"
    createBucket(frontendBucket)

    externalPublishBucketConfig.foreach(c => createBucket(c.bucket.value))
    val bucketToRole =
      externalPublishBucketConfig.map(c => c.bucket -> c.roleArn).toMap

    (
      new AlpakkaS3StreamClient(
        s3Presigner,
        s3Client,
        stsClient,
        Region.US_EAST_1,
        S3Bucket(frontendBucket),
        "dataset-assets",
        chunkSize,
        bucketToRole
      ),
      publishBucket,
      frontendBucket
    )
  }

  "S3 zip stream source" should {
    "generate stream from S3 bucket" in {
      val (client, bucket, _) = createClient()

      // Data is even multiple of chunk size
      putObject(bucket, "0/1/files/file1.txt", "file1 data")
      // Not a multiple - last chunk is truncated
      putObject(bucket, "0/1/files/file2.txt", "file2 data+")
      putObject(bucket, "0/1/files/nested/file3.txt", "file3 data")

      val sink = client
        .datasetFilesSource(version(0, 1, bucket), "dataset", None)
        .mapAsync(1) {
          case (path, source) =>
            source.runWith(Sink.fold(ByteString.empty)(_ ++ _)).map((path, _))
        }
        .toMat(TestSink.probe)(Keep.right)
        .run()

      sink.request(n = 100)
      sink.expectNextUnordered(
        ("dataset/files/file1.txt", ByteString("file1 data")),
        ("dataset/files/file2.txt", ByteString("file2 data+")),
        ("dataset/files/nested/file3.txt", ByteString("file3 data"))
      )
      sink.expectComplete()
    }

    "ignore other keys" in {
      val (client, bucket, _) = createClient()
      putObject(bucket, "0/1/files/file1.txt", "file1 data")
      putObject(bucket, "5/67/files/file7329.txt", "some other stuff")

      val sink = client
        .datasetFilesSource(version(0, 1, bucket), "dataset", None)
        .mapAsync(1) {
          case (path, source) =>
            source.runWith(Sink.fold(ByteString.empty)(_ ++ _)).map((path, _))
        }
        .toMat(TestSink.probe)(Keep.right)
        .run()

      sink.request(n = 100)
      sink.expectNext(("dataset/files/file1.txt", ByteString("file1 data")))
      sink.expectComplete()
    }

    "generate stream from external S3 publish bucket" in {
      val externalBucket = s"external-bucket-${UUID.randomUUID()}"
      val externalBucketConfig = ExternalPublishBucketConfiguration(
        S3Bucket(externalBucket),
        Arn.fromString(
          "arn:aws:iam::000000000000:role/external-bucket-access-role"
        )
      )
      val (client, _, _) =
        createClient(externalPublishBucketConfig = Some(externalBucketConfig))

      // Data is even multiple of chunk size
      putObject(externalBucket, "0/1/files/file1.txt", "file1 data")
      // Not a multiple - last chunk is truncated
      putObject(externalBucket, "0/1/files/file2.txt", "file2 data+")
      putObject(externalBucket, "0/1/files/nested/file3.txt", "file3 data")

      val sink = client
        .datasetFilesSource(version(0, 1, externalBucket), "dataset", None)
        .mapAsync(1) {
          case (path, source) =>
            source.runWith(Sink.fold(ByteString.empty)(_ ++ _)).map((path, _))
        }
        .toMat(TestSink.probe)(Keep.right)
        .run()

      sink.request(n = 100)
      sink.expectNextUnordered(
        ("dataset/files/file1.txt", ByteString("file1 data")),
        ("dataset/files/file2.txt", ByteString("file2 data+")),
        ("dataset/files/nested/file3.txt", ByteString("file3 data"))
      )
      sink.expectComplete()
    }
  }

  "S3 metadata source" should {
    "stream manifest.json from S3 bucket" in {
      val (client, bucket, _) = createClient()

      putObject(bucket, "0/1/manifest.json", "{\"name\": \"Test Dataset\"}")

      val (source, contentLength) = client
        .datasetMetadataSource(version(0, 1, bucket))
        .awaitFinite()

      source
        .runWith(Sink.fold(ByteString.empty)(_ ++ _))
        .map(_.utf8String)
        .awaitFinite() shouldBe "{\"name\": \"Test Dataset\"}"

      contentLength shouldBe 24
    }
  }

  "S3 readme source" should {
    "stream readme.md from S3 bucket" in {
      val (client, bucket, _) = createClient()
      putObject(bucket, "0/1/readme.md", "This is a description")

      val readme = client
        .readDatasetReadme(version(0, 1, bucket), None)
        .awaitFinite()

      readme shouldBe Readme("This is a description")
    }

    "stream readme.md for revision from S3 bucket" in {
      val (client, bucket, _) = createClient()
      putObject(
        bucket,
        "0/1/revisions/2/readme.md",
        "This is a revised description"
      )

      val readme = client
        .readDatasetReadme(version(0, 1, bucket), Some(revision(0, 1, 2)))
        .awaitFinite()

      readme shouldBe Readme("This is a revised description")
    }
  }

  "S3 temporary publish results" should {
    "be available and parsable, and be deleted" in {
      val (client, bucket, _) = createClient()

      val tempResult =
        PublishJobOutput(
          S3Key.Version(0, 1) / "readme.md",
          S3Key.Version(0, 1) / "banner.jpg",
          totalSize = 76543
        )
      putObject(bucket, "0/1/outputs.json", tempResult.asJson.toString)

      client
        .readPublishJobOutput(version(0, 1, bucket))
        .awaitFinite() shouldBe tempResult

      client
        .deletePublishJobOutput(version(0, 1, bucket))
        .awaitFinite()

      // Should no longer exist
      whenReady(
        client
          .readPublishJobOutput(version(0, 1, bucket))
          .failed
      ) { _ shouldBe an[S3Exception] }
    }
  }

  def dataset(id: Int): PublicDataset = PublicDataset(
    id = id,
    name = "Colors",
    sourceOrganizationId = 5,
    sourceOrganizationName = "SPARC",
    sourceDatasetId = 579,
    ownerId = 27,
    ownerFirstName = "John",
    ownerLastName = "Coltrane",
    ownerOrcid = "0000-0001-2345-6789",
    license = License.MIT,
    tags = List("red", "green")
  )

  def version(
    datasetId: Int,
    version: Int,
    bucket: String
  ): PublicDatasetVersion =
    PublicDatasetVersion(
      datasetId = datasetId,
      version = version,
      size = 100L,
      description = "red green blue ganglia",
      modelCount = Map.empty,
      fileCount = 0L,
      recordCount = 0L,
      s3Bucket = S3Bucket(bucket),
      s3Key = S3Key.Version(datasetId, version),
      status = PublishStatus.PublishSucceeded,
      doi = "10.21397/abcd-1234",
      schemaVersion = PennsieveSchemaVersion.latest,
      banner = None,
      readme = None,
      executionArn = None,
      embargoReleaseDate = None,
      createdAt = OffsetDateTime.now,
      updatedAt = OffsetDateTime.now
    )

  def revision(datasetId: Int, version: Int, revision: Int): Revision =
    Revision(
      datasetId = datasetId,
      version = version,
      revision = revision,
      createdAt = OffsetDateTime.now,
      updatedAt = OffsetDateTime.now
    )

  def contributors(datasetId: Int, version: Int) = List(
    PublicContributor(
      firstName = "Alan",
      middleInitial = Some("M"),
      lastName = "Turing",
      degree = Some(Degree.PhD),
      orcid = None,
      datasetId = datasetId,
      versionId = version,
      sourceContributorId = 781,
      sourceUserId = Some(8849)
    )
  )

  def collections(datasetId: Int, version: Int) = List(
    PublicCollection(
      name = "My amazing collection",
      sourceCollectionId = 1,
      datasetId = datasetId,
      versionId = version
    )
  )

  def externalPublications(datasetId: Int, version: Int) = List(
    PublicExternalPublication(
      doi = "10.26275/v62f-qd4v",
      relationshipType = RelationshipType.Describes,
      datasetId = datasetId,
      version = version
    )
  )

  "S3 records source" should {
    "generate a stream of records" in {
      val (client, bucket, _) = createClient()

      putObject(
        bucket,
        "3/4/metadata/schema.json",
        getResource("/metadata/schema.json")
      )

      putObject(
        bucket,
        "3/4/metadata/samples.csv",
        getResource("/metadata/samples.csv")
      )

      val records =
        client
          .datasetRecordSource(dataset(3), version(3, 4, bucket))
          .runWith(Sink.seq)
          .awaitFinite()

      records.length shouldBe 7
      records.head shouldBe Record(
        model = "samples",
        datasetId = 3,
        version = 4,
        organizationName = "SPARC",
        properties = Map(
          "id" -> "891f6702-538b-495a-b027-c35341f2dc57",
          "label" -> "samples",
          "neuron_size" -> "Unknown",
          "number_of_neurons" -> "",
          "sample_id" -> "HB_50A-2"
        )
      )
    }

    "generate an empty stream of records when the dataset does not have graph data" in {
      val (client, bucket, _) = createClient()

      client
        .datasetRecordSource(dataset(3), version(3, 4, bucket))
        .runWith(Sink.seq)
        .awaitFinite() shouldBe empty
    }

  }

  "S3 write revision " should {
    "write metadata, banner, and readme to revisions folder" in {
      val (client, publishBucket, frontendBucket) = createClient()

      val datasetAssetsBucket = "dataset-assets-bucket"
      createBucket(datasetAssetsBucket)

      putObject(
        datasetAssetsBucket,
        "0ab5ac9f-47cd-4544-8418-0f0ae4600779/a-dataset-banner.jpg",
        getResource("/banner.jpg")
      )

      val bannerPresignedUrl = getPresignedUrl(
        datasetAssetsBucket,
        "0ab5ac9f-47cd-4544-8418-0f0ae4600779/a-dataset-banner.jpg"
      )

      putObject(
        datasetAssetsBucket,
        "5669a776-d369-4692-ba1a-9f60fcabbf03/readme.md",
        getResource("/readme.md")
      )

      val readmePresignedUrl = getPresignedUrl(
        datasetAssetsBucket,
        "5669a776-d369-4692-ba1a-9f60fcabbf03/readme.md"
      )

      val newFiles = client
        .writeDatasetRevisionMetadata(
          dataset(3),
          version(3, 4, publishBucket),
          contributors(3, 4),
          revision(3, 4, 5),
          collections(3, 4),
          externalPublications(3, 4),
          bannerPresignedUrl = bannerPresignedUrl,
          readmePresignedUrl = readmePresignedUrl
        )
        .awaitFinite()

      newFiles shouldBe client.NewFiles(
        banner = FileManifest(
          path = "revisions/5/banner.jpg",
          size = 43649,
          fileType = FileType.JPEG,
          None
        ),
        readme = FileManifest(
          path = "revisions/5/readme.md",
          size = 29,
          fileType = FileType.Markdown,
          None
        ),
        manifest = FileManifest(
          path = "revisions/5/manifest.json",
          size = 902,
          fileType = FileType.Json,
          None
        )
      )
      val readme = getObject(publishBucket, "3/4/revisions/5/readme.md")
      readme shouldBe readResourceContents("/readme.md")

      val banner = getObject(publishBucket, "3/4/revisions/5/banner.jpg")
      banner shouldBe readResourceContents("/banner.jpg")

      val manifest = StandardCharsets.UTF_8
        .decode(getObject(publishBucket, "3/4/revisions/5/manifest.json"))
        .toString

      // Should drop the empty "files" key.
      parse(manifest).value.hcursor.keys.get should not contain "files"

      // However, not having "files" makes decoding the dataset metadata a little painful.
      // TODO: make "files" optional in the root case class and remove this.
      implicit val decoder: Decoder[DatasetMetadataV4_0] =
        deriveDecoder[DatasetMetadataV4_0].prepare(
          _.withFocus(_.mapObject(_.add("files", List.empty[String].asJson)))
        )

      decode[DatasetMetadataV4_0](manifest).value shouldBe DatasetMetadataV4_0(
        pennsieveDatasetId = 3,
        version = 4,
        revision = Some(5),
        name = "Colors",
        description = "red green blue ganglia",
        creator = PublishedContributor(
          "John",
          "Coltrane",
          orcid = Some("0000-0001-2345-6789")
        ),
        contributors = List(PublishedContributor("Alan", "Turing", None)),
        sourceOrganization = "SPARC",
        keywords = List("red", "green"),
        datePublished = OffsetDateTime.now.toLocalDate(),
        license = Some(License.MIT),
        `@id` = "10.21397/abcd-1234",
        collections = Some(List(PublishedCollection("My amazing collection"))),
        relatedPublications = Some(
          List(
            PublishedExternalPublication(
              doi = Doi("10.26275/v62f-qd4v"),
              Some(RelationshipType.Describes)
            )
          )
        )
      )

      // Should also copy assets to the frontend bucket

      val frontendReadme =
        getObject(frontendBucket, "dataset-assets/3/4/revisions/5/readme.md")
      frontendReadme shouldBe readResourceContents("/readme.md")

      val frontendBanner =
        getObject(frontendBucket, "dataset-assets/3/4/revisions/5/banner.jpg")
      frontendBanner shouldBe readResourceContents("/banner.jpg")
    }
  }

  def createBucket(name: String) =
    s3Client.createBucket(CreateBucketRequest.builder().bucket(name).build())

  def putObject(bucket: String, key: String, content: String = randomString()) =
    s3Client.putObject(
      PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build(),
      RequestBody.fromString(content)
    )

  def putObject(bucket: String, key: String, path: Path) =
    s3Client.putObject(
      PutObjectRequest
        .builder()
        .bucket(bucket)
        .key(key)
        .build(),
      RequestBody.fromFile(path)
    )

  def getObject(bucket: String, key: String): ByteBuffer = {
    s3Client
      .getObjectAsBytes(
        GetObjectRequest.builder().bucket(bucket).key(key).build()
      )
      .asByteBuffer()
  }

  def getPresignedUrl(bucket: String, key: String): Uri =
    Uri(
      s3Presigner
        .presignGetObject(
          GetObjectPresignRequest
            .builder()
            .signatureDuration(10.minutes.toJava)
            .getObjectRequest(
              GetObjectRequest
                .builder()
                .bucket(bucket)
                .key(key)
                .build()
            )
            .build()
        )
        .url()
        .toString
    )

  def getResource(name: String): Path = {
    val resource = getClass.getResource(name)
    if (resource == null) throw new Exception(s"Resource $name does not exist")
    Paths.get(resource.toURI())
  }

  def readResourceContents(name: String): ByteBuffer =
    FileIO
      .fromPath(getResource(name))
      .runWith(Sink.fold(ByteString())(_ ++ _))
      .awaitFinite()
      .asByteBuffer

  def randomString(length: Int = 20): String =
    Random.alphanumeric take length mkString

}
