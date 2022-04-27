// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.pennsieve.discover.clients._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db.{
  PublicContributorsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFilesMapper
}
import com.pennsieve.discover.notifications.SQSNotificationHandler
import com.pennsieve.discover.models._
import com.pennsieve.service.utilities.SingleHttpResponder
import com.pennsieve.test.AwaitableImplicits
import com.spotify.docker.client.DefaultDockerClient
import com.spotify.docker.client.exceptions.DockerException
import com.typesafe.scalalogging.StrictLogging
import com.whisk.docker.DockerFactory
import com.whisk.docker.impl.spotify.SpotifyDockerFactory
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.time.{ Second, Seconds, Span }
import org.scalatest.{
  BeforeAndAfterAll,
  BeforeAndAfterEach,
  OptionValues,
  Suite
}
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.regions.Region
import squants.information.InformationConversions._
import java.net.URI

import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

trait ServiceSpecHarness
    extends Suite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with DockerPostgresService
    with DockerTestKit
    with AwaitableImplicits
    with OptionValues
    with StrictLogging { suite: Suite =>

  implicit private val system: ActorSystem = ActorSystem("discover-service")
  implicit private val materializer: ActorMaterializer = ActorMaterializer()
  implicit private val executionContext: ExecutionContext = system.dispatcher

  override val PullImagesTimeout: FiniteDuration = 5.minutes
  override val StartContainersTimeout: FiniteDuration = 120.seconds
  override val StopContainersTimeout: FiniteDuration = 120.seconds

  // increase default patience to allow containers to come up
  implicit val patience: PatienceConfig =
    PatienceConfig(Span(60, Seconds), Span(1, Second))

  // provide a dockerFactory
  override implicit val dockerFactory: DockerFactory =
    try new SpotifyDockerFactory(DefaultDockerClient.fromEnv().build())
    catch {
      case _: DockerException =>
        throw new DockerException("Docker may not be running")
    }

  implicit val config: Config =
    Config(
      host = "0.0.0.0",
      port = 8080,
      publicUrl = "https://discover.pennsieve.org",
      assetsUrl = "https://assets.discover.pennsieve.org",
      postgres = postgresConfiguration,
      jwt = JwtConfig("test-key"),
      doiService =
        DoiServiceClientConfiguration("https://mock-doi-service-host"),
      awsStepFunctions = StepFunctionsConfiguration(
        "arn:aws:states:us-east-1:publish-123345",
        "arn:aws:states:us-east-1:release-123345",
        Region.US_EAST_1
      ),
      awsLambda = LambdaConfiguration(
        lambdaFunction = "test-s3clean-lambda",
        region = Region.US_EAST_1,
        parallelism = 1
      ),
      awsElasticSearch =
        ElasticSearchConfiguration(host = "http://localhost", port = 9200),
      s3 = S3Configuration(
        region = Region.US_EAST_1,
        publishBucket = S3Bucket("bucket"),
        frontendBucket = S3Bucket("frontend-bucket"),
        assetsKeyPrefix = "dataset-assets",
        embargoBucket = S3Bucket("embargo-bucket"),
        publishLogsBucket = S3Bucket("publish-log-bucket"),
        accessLogsPath = "/logs/s3",
        chunkSize = 20.megabytes
      ),
      sqs = SQSConfiguration(
        queueUrl = "http://localhost:9324/queue/test-queue",
        region = Region.US_EAST_1,
        parallelism = 1
      ),
      sns =
        SNSConfiguration(alertTopic = "test-victorops-topic", Region.US_EAST_1),
      pennsieveApi =
        PennsieveApiConfiguration("https:/dev-api-use1.pennsieve.io"),
      authorizationService = AuthorizationConfiguration(
        "https:/dev-authorization-service-use1.pennsieve.io"
      ),
      download = DownloadConfiguration(512.megabytes, 5.megabytes)
    )

  def getPorts(config: Config): Ports = {
    val doiClient: DoiClient =
      new MockDoiClient(
        new SingleHttpResponder().responder,
        executionContext,
        materializer
      )

    val athenaClient: AthenaClient =
      new MockAthenaClient()

    val stepFunctionsClient: StepFunctionsClient =
      new MockStepFunctionsClient()

    val lambdaClient: LambdaClient = new MockLambdaClient()

    val s3StreamClient: S3StreamClient =
      new MockS3StreamClient()

    val searchClient: SearchClient =
      new MockSearchClient()

    val pennsieveApiClient: PennsieveApiClient =
      new MockPennsieveApiClient()

    val authorizationClient: AuthorizationClient =
      new MockAuthorizationClient(config.jwt.key)

    val victorOpsClient = new MockVictorOpsClient()

    val sqsClient = SqsAsyncClient
      .builder()
      .httpClientBuilder(NettyNioAsyncHttpClient.builder())
      .region(config.sqs.region)
      .endpointOverride(new URI("https://localhost"))
      .build()

    Ports(config).copy(
      doiClient = doiClient,
      stepFunctionsClient = stepFunctionsClient,
      lambdaClient = lambdaClient,
      s3StreamClient = s3StreamClient,
      searchClient = searchClient,
      pennsieveApiClient = pennsieveApiClient,
      authorizationClient = authorizationClient,
      victorOpsClient = victorOpsClient,
      sqsClient = sqsClient,
      athenaClient = athenaClient
    )
  }

  lazy implicit val ports: Ports = getPorts(config)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val setup = isContainerReady(postgresContainer).map { _ =>
      DatabaseMigrator.run(config.postgres)
    }

    Await.result(setup, 30.seconds)
  }

  override def afterAll(): Unit = {
    ports.db.close()
    super.afterAll()
  }

  override def afterEach(): Unit = {
    ports.stepFunctionsClient
      .asInstanceOf[MockStepFunctionsClient]
      .clear()

    ports.pennsieveApiClient
      .asInstanceOf[MockPennsieveApiClient]
      .clear()

    ports.doiClient
      .asInstanceOf[MockDoiClient]
      .dois
      .clear()

    ports.victorOpsClient
      .asInstanceOf[MockVictorOpsClient]
      .sentAlerts
      .clear()

    ports.searchClient
      .asInstanceOf[MockSearchClient]
      .clear()

    ports.s3StreamClient
      .asInstanceOf[MockS3StreamClient]
      .clear()

    // Clear dataset tables
    ports.db
      .run(for {
        _ <- PublicContributorsMapper.delete
        _ <- PublicDatasetsMapper.delete
        _ <- PublicDatasetVersionsMapper.delete
        _ <- PublicFilesMapper.delete
      } yield ())
      .awaitFinite()

    super.afterEach()
  }
}
