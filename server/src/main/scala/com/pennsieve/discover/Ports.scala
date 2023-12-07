// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.db.profile.api._
import com.pennsieve.auth.middleware.Jwt
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.Materializer._
import com.pennsieve.discover.clients.{
  AlpakkaLambdaClient,
  AlpakkaS3StreamClient,
  AthenaClient,
  AthenaClientImpl,
  AuthorizationClient,
  AuthorizationClientImpl,
  AwsElasticSearchClient,
  AwsStepFunctionsClient,
  DoiClient,
  HttpClient,
  LambdaClient,
  PennsieveApiClient,
  PennsieveApiClientImpl,
  S3StreamClient,
  SearchClient,
  StepFunctionsClient
}
import com.pennsieve.service.utilities.{
  ContextLogger,
  LogContext,
  SingleHttpResponder
}
import com.typesafe.scalalogging.LoggerTakingImplicit
import com.zaxxer.hikari.HikariDataSource
import slick.util.AsyncExecutor
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.concurrent.ExecutionContext

case class Ports(
  config: Config,
  jwt: Jwt.Config,
  db: Database,
  doiClient: DoiClient,
  stepFunctionsClient: StepFunctionsClient,
  lambdaClient: LambdaClient,
  s3StreamClient: S3StreamClient,
  searchClient: SearchClient,
  pennsieveApiClient: PennsieveApiClient,
  authorizationClient: AuthorizationClient,
  sqsClient: SqsAsyncClient,
  athenaClient: AthenaClient
) {
  val logger: ContextLogger = new ContextLogger()
  val log: LoggerTakingImplicit[LogContext] = logger.context
}

object Ports {

  def apply(
    config: Config
  )(implicit
    system: ActorSystem,
    executionContext: ExecutionContext
  ): Ports = {

    val jwt: Jwt.Config = new Jwt.Config {
      val key: String = config.jwt.key
    }

    val db: Database = {
      val hikariDataSource = new HikariDataSource()

      hikariDataSource.setJdbcUrl(config.postgres.jdbcURL)
      hikariDataSource.setUsername(config.postgres.user)
      hikariDataSource.setPassword(config.postgres.password)
      hikariDataSource.setMaximumPoolSize(config.postgres.numConnections)
      hikariDataSource.setDriverClassName(config.postgres.driver)
      hikariDataSource.setSchema(config.postgres.schema)
      hikariDataSource.setConnectionInitSql("set time zone 'UTC'")

      // Currently minThreads, maxThreads and maxConnections MUST be the same value
      // https://github.com/slick/slick/issues/1938
      Database.forDataSource(
        hikariDataSource,
        maxConnections = None, // Ignored if an executor is provided
        executor = AsyncExecutor(
          name = "AsyncExecutor.pennsieve",
          minThreads = config.postgres.numConnections,
          maxThreads = config.postgres.numConnections,
          maxConnections = config.postgres.numConnections,
          queueSize = config.postgres.queueSize
        )
      )
    }

    val doiClient: DoiClient = {
      val client = new SingleHttpResponder().responder
      new DoiClient(config.doiService.host)(client, executionContext, system)
    }

    val stepFunctionsClient: StepFunctionsClient = new AwsStepFunctionsClient(
      publishStateMachineArn = config.awsStepFunctions.publishStateMachineArn,
      releaseStateMachineArn = config.awsStepFunctions.releaseStateMachineArn,
      config.awsStepFunctions.region
    )

    val lambdaClient: LambdaClient = new AlpakkaLambdaClient(
      s3CleanFunction = config.awsLambda.lambdaFunction,
      region = config.awsLambda.region,
      parallelism = config.awsLambda.parallelism
    )

    val s3StreamClient: S3StreamClient =
      AlpakkaS3StreamClient(
        region = config.s3.region,
        frontendBucket = config.s3.frontendBucket,
        assetsKeyPrefix = config.s3.assetsKeyPrefix,
        chunkSize = config.s3.chunkSize,
        externalPublishBucketToRole = config.externalPublishBuckets
      )

    val searchClient: SearchClient =
      new AwsElasticSearchClient(config.awsElasticSearch.elasticUri, config)

    val pennsieveApiClient: PennsieveApiClient =
      new PennsieveApiClientImpl(config.pennsieveApi.host, jwt, HttpClient())

    val authorizationClient: AuthorizationClient =
      new AuthorizationClientImpl(
        config.authorizationService.host,
        HttpClient()
      )

    val sqsClient: SqsAsyncClient = SqsAsyncClient
      .builder()
      .httpClientBuilder(NettyNioAsyncHttpClient.builder())
      .region(config.sqs.region)
      .build()

    val athenaClient: AthenaClient = new AthenaClientImpl(
      pennsieveTable = config.athena.pennsieveBucketAccessTable,
      sparcTable = config.athena.sparcBucketAccessTable
    )

    Ports(
      config,
      jwt,
      db,
      doiClient,
      stepFunctionsClient,
      lambdaClient,
      s3StreamClient,
      searchClient,
      pennsieveApiClient,
      authorizationClient,
      sqsClient,
      athenaClient
    )
  }
}
