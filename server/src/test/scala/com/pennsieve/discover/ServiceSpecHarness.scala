// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import akka.actor.ActorSystem
import com.pennsieve.discover.clients._
import com.pennsieve.discover.db.profile.api._
import com.pennsieve.discover.db.{
  DatasetDownloadsMapper,
  PublicContributorsMapper,
  PublicDatasetVersionsMapper,
  PublicDatasetsMapper,
  PublicFilesMapper,
  WorkspaceSettingsMapper
}
import com.pennsieve.discover.models._
import com.pennsieve.discover.testcontainers.DockerContainers.postgresContainer.postgresConfiguration
import com.pennsieve.discover.testcontainers.PostgresDockerContainer
import com.pennsieve.service.utilities.SingleHttpResponder
import com.pennsieve.test.{ AwaitableImplicits, PersistantTestContainers }
import com.typesafe.scalalogging.StrictLogging
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
import java.util.TimeZone
import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.duration.{ DurationInt, FiniteDuration }

trait ServiceSpecHarness
    extends Suite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with PersistantTestContainers
    with PostgresDockerContainer
    with AwaitableImplicits
    with OptionValues
    with StrictLogging { suite: Suite =>

  // system is deliberately left uninitialized here to avoid conflicts with
  // other commonly used traits that provide an ActorSystem. i.e., ScalatestRouteTest.
  // If a class extends both this trait and ScalatestRouteTest, then
  // they do not need to do anything. The system will be supplied by ScalatestRouteTest.
  // But tests that do not extend ScalatestRouteTest or another trait that provides an implicit
  // ActorSystem, should override this value by initializing it themselves. See PublicDatasetVersionsMapperSpec
  // for example.
  implicit def system: ActorSystem
  implicit def executionContext: ExecutionContext = system.dispatcher

  implicit var config: Config = _

  def getPorts(config: Config): Ports = {
    val doiClient: DoiClient =
      new MockDoiClient(
        new SingleHttpResponder().responder,
        executionContext,
        system
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
      sqsClient = sqsClient,
      athenaClient = athenaClient
    )
  }

  implicit var ports: Ports = _

  override def afterStart(): Unit = {
    super.afterStart()

    config = Config(
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
        chunkSize = 20.megabytes,
        publish50Bucket = S3Bucket("publish-bucket-50"),
        embargo50Bucket = S3Bucket("embargo-bucket-50")
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
      download = DownloadConfiguration(512.megabytes, 5.megabytes),
      athena = AthenaConfig(
        pennsieveBucketAccessTable = "s3_access_logs_db.discover",
        sparcBucketAccessTable =
          "sparc_glue_catalog.dev_s3_access_logs_db.discover",
        rejoinBucketAccessTable =
          "rejoin_glue_catalog.dev_s3_access_logs_db.discover",
        sparcAodBucketAccessTable =
          "sparc_aod_glue_catalog.dev_s3_access_logs_db.discover"
      ),
      runtimeSettings = RuntimeSettings(deleteReleaseIntermediateFile = false),
      doiCollections = DoiCollections(
        pennsieveDoiPrefix = "10.00000",
        IdSpace(99999, "Test Collections ID Space")
      )
    )
    ports = getPorts(config)

    DatabaseMigrator.run(config.postgres)

  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Setting the timezone for tests that compare times deserialized from the DB.
    // This is really only to help when running tests locally, because the JVM timezone
    // is already UTC on the servers and in Jenkins.
    // The underlying issue is that the Postgres type of our datetime columns is timestamp,
    // so no timezone info, but the Java type is OffsetDateTime which does include timezone info.
    // And we have no slick mapper configured to make sure all serialization/deserialization between
    // Postgres and Scala uses UTC, so things can go wrong when running the tests locally in a non-UTC timezone.
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

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
      .clear()

    ports.searchClient
      .asInstanceOf[MockSearchClient]
      .clear()

    ports.s3StreamClient
      .asInstanceOf[MockS3StreamClient]
      .clear()

    ports.lambdaClient
      .asInstanceOf[MockLambdaClient]
      .requests
      .clear()

    ports.athenaClient
      .asInstanceOf[MockAthenaClient]
      .reset()

    // Clear dataset tables
    ports.db
      .run(for {
        _ <- PublicContributorsMapper.delete
        _ <- PublicDatasetsMapper.delete
        _ <- PublicDatasetVersionsMapper.delete
        _ <- PublicFilesMapper.delete
        _ <- WorkspaceSettingsMapper.delete
        _ <- DatasetDownloadsMapper.delete
      } yield ())
      .awaitFinite()

    super.afterEach()
  }
}
