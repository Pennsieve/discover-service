// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import com.pennsieve.discover.models._
import com.pennsieve.discover.{
  DockerMockServerService,
  ExternalPublishBucketConfiguration
}
import com.pennsieve.models._
import com.pennsieve.test.AwaitableImplicits
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.exceptions.DockerException
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.mockserver.model.{ HttpRequest, MediaType, RequestDefinition }
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.Inspectors.forAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.sts.StsClient
import squants.information.Information
import squants.information.InformationConversions._

import java.net.URI
import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.Random

class MockServerS3StreamClientSpec
    extends AnyWordSpec
    with Matchers
    with AwaitableImplicits
    with ScalaFutures
    with DockerMockServerService
    with DockerTestKit {

  // alpakka-s3 v1.0 can only be configured via Typesafe config passed to the
  // actor system, or as S3Settings that are attached to every graph
  lazy val config = ConfigFactory
    .load()
    .withValue(
      "alpakka.s3.endpoint-url",
      ConfigValueFactory.fromAnyRef(mockServerEndpoint)
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
    .endpointOverride(new URI(mockServerEndpoint))
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
    .endpointOverride(new URI(mockServerEndpoint))
    .build()

  lazy val stsClient: StsClient = StsClient
    .builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(
      StaticCredentialsProvider
        .create(AwsBasicCredentials.create(accessKey, secretKey))
    )
    .endpointOverride(new URI(mockServerEndpoint))
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

    val frontendBucket = s"frontend-bucket-${UUID.randomUUID()}"

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

  "copyPresignedUrlToRevision" should {
    "set the requester pays header in all requests" in {
      val (client, bucket, _) = createClient()

      val presignedUrlPath =
        "/19/1722/17e6c6f2-f85a-474a-9df3-3e5b0d8e9bd3/dataset_banner_1722.jpg"
      val presignedUrl = Uri(s"${mockServerEndpoint}${presignedUrlPath}")
      val key = S3Key.File("123/1/banner.jpg")

      mockServerClient
        .when(
          request()
            .withMethod("GET")
            .withPath(presignedUrlPath)
        )
        .respond(
          response()
            .withBody("some_response_body")
        )

      /*val uploadId = randomString
      val startUploadRequest =
        akkaStartMultipartExpectation(bucket, key, uploadId)
      val uploadPartRequest = akkaUploadPartExpectation(bucket, key)
      val completeUploadRequest =
        akkaCompleteMultipartExpectation(bucket, key)
       */

      val putObjectRequest = putObjectExpectation(bucket, key)
      val headRequest = akkaHeadExpectation(bucket, key)

      client
        .copyPresignedUrlToRevision(presignedUrl, key, version(123, 1, bucket))
        .awaitFinite()

      /*assertRequestsAreRequesterPays(startUploadRequest)
      assertRequestsAreRequesterPays(uploadPartRequest)
      assertRequestsAreRequesterPays(completeUploadRequest)
       */

      assertRequestsAreRequesterPays(putObjectRequest)
      assertRequestsAreRequesterPays(headRequest)

    }
  }

  def randomString: String =
    Random.alphanumeric.filter(_.isLetter).take(10).mkString

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

  private def assertRequestsAreRequesterPays(
    request: RequestDefinition
  ): Unit = {
    forAll(mockServerClient.retrieveRecordedRequests(request).toSeq) {
      _.containsHeader("x-amz-request-payer", "requester") should be(true)
    }
  }

  private def akkaStartMultipartExpectation(
    bucket: String,
    key: S3Key,
    uploadId: String
  ): HttpRequest = {
    val requestMatcher = request()
      .withMethod("POST")
      .withPath(s"/$bucket/${key}")
      .withQueryStringParameter("uploads")

    mockServerClient
      .when(requestMatcher)
      .respond(
        response
          .withStatusCode(200)
          .withContentType(MediaType.APPLICATION_XML)
          .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
               |            <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
               |              <Bucket>$bucket</Bucket>
               |              <Key>${key}</Key>
               |              <UploadId>${uploadId}</UploadId>
               |            </InitiateMultipartUploadResult>""".stripMargin)
      )
    requestMatcher

  }

  private def akkaUploadPartExpectation(
    bucket: String,
    key: S3Key
  ): HttpRequest = {
    val etag = s""""$randomString""""
    val requestMatcher = request()
      .withMethod("PUT")
      .withPath(s"/$bucket/${key}")
      .withQueryStringParameter("partNumber")
      .withQueryStringParameter("uploadId")
    mockServerClient
      .when(requestMatcher)
      .respond(
        response
          .withStatusCode(200)
          .withHeader("ETag", etag)
      )

    requestMatcher

  }

  private def putObjectExpectation(bucket: String, key: S3Key): HttpRequest = {
    val requestMatcher = request()
      .withMethod("PUT")
      .withPath(s"/$bucket/${key}")
    mockServerClient
      .when(requestMatcher)
      .respond(
        response
          .withStatusCode(200)
      )

    requestMatcher

  }

  private def akkaCompleteMultipartExpectation(
    bucket: String,
    key: S3Key
  ): HttpRequest = {

    val requestMatcher = request()
      .withMethod("POST")
      .withPath(s"/$bucket/${key}")
      .withQueryStringParameter("uploadId")
    mockServerClient
      .when(requestMatcher)
      .respond(
        response
          .withStatusCode(200)
          .withContentType(MediaType.APPLICATION_XML)
          .withBody(s"""<?xml version="1.0" encoding="UTF-8"?>
                   |            <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                   |             <Location>https://$bucket.s3.amazonaws.com/${key}</Location>
                   |             <Bucket>$bucket</Bucket>
                   |             <Key>${key}</Key>
                   |             <ETag>"${randomString}"</ETag>
                   |            </CompleteMultipartUploadResult>""".stripMargin)
      )

    requestMatcher

  }

  private def akkaHeadExpectation(bucket: String, key: S3Key): HttpRequest = {
    val requestMatcher = request()
      .withMethod("HEAD")
      .withPath(s"/$bucket/${key}")

    mockServerClient
      .when(requestMatcher)
      .respond(
        response
          .withStatusCode(200)
          .withHeader("content-length", "567")
      )
    requestMatcher

  }

}
