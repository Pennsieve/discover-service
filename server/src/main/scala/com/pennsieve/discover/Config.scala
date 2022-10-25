// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.discover.models.S3Bucket
import software.amazon.awssdk.regions.Region
import squants.information.Information
import squants.information.InformationConversions._
import pureconfig._
import pureconfig.module.squants._
import pureconfig.generic.auto._
import software.amazon.awssdk.arns.Arn

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

case class Config(
  host: String = "0.0.0.0",
  port: Int = 8080,
  publicUrl: String,
  assetsUrl: String,
  postgres: PostgresConfiguration,
  jwt: JwtConfig,
  doiService: DoiServiceClientConfiguration,
  awsStepFunctions: StepFunctionsConfiguration,
  awsLambda: LambdaConfiguration,
  awsElasticSearch: ElasticSearchConfiguration,
  s3: S3Configuration,
  sqs: SQSConfiguration,
  sns: SNSConfiguration,
  pennsieveApi: PennsieveApiConfiguration,
  authorizationService: AuthorizationConfiguration,
  download: DownloadConfiguration,
  externalPublishBuckets: Map[S3Bucket, Arn] = Map.empty
)

object Config {
  implicit val awsRegionReader = ConfigReader[String].map(Region.of(_))
  implicit val awsArnReader = ConfigReader[String].map(Arn.fromString(_))
  implicit val externalPublishBucketConfigurationReader
    : ConfigReader[Map[S3Bucket, Arn]] =
    ConfigReader[List[ExternalPublishBucketConfiguration]]
      .map(_.map(c => c.bucket -> c.roleArn).toMap)

  def load: Config = ConfigSource.default.loadOrThrow[Config]
}

case class JwtConfig(key: String, duration: FiniteDuration = 5.minutes)

case class PostgresConfiguration(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  numConnections: Int = 20,
  queueSize: Int = 1000,
  driver: String = "org.postgresql.Driver",
  schema: String = "discover",
  useSSL: Boolean = true
) {
  private val jdbcBaseURL: String = s"jdbc:postgresql://$host:$port/$database"
  final val jdbcURL = {
    if (useSSL) jdbcBaseURL + "?ssl=true&sslmode=verify-ca"
    else jdbcBaseURL
  }
}

case class DoiServiceClientConfiguration(host: String)

case class StepFunctionsConfiguration(
  publishStateMachineArn: String,
  releaseStateMachineArn: String,
  region: Region
)

case class S3Configuration(
  region: Region,
  publishBucket: S3Bucket,
  // Readme and banner assets are stored in the same bucket that serves the
  // frontend site.  This prefix is used to distinguish them from other things
  // in the bucket.
  frontendBucket: S3Bucket,
  publishLogsBucket: S3Bucket,
  assetsKeyPrefix: String,
  embargoBucket: S3Bucket,
  accessLogsPath: String,
  chunkSize: Information = 20.megabytes
)

case class DownloadConfiguration(
  maxSize: Information,
  ratePerSecond: Information
)

case class SQSConfiguration(region: Region, queueUrl: String, parallelism: Int)

case class PennsieveApiConfiguration(host: String)

case class AuthorizationConfiguration(host: String)

case class LambdaConfiguration(
  lambdaFunction: String,
  region: Region,
  parallelism: Int
)

case class ElasticSearchConfiguration(host: String, port: Int) {
  val elasticUri: String = s"$host:$port"
}

case class SNSConfiguration(alertTopic: String, region: Region)

case class ExternalPublishBucketConfiguration(bucket: S3Bucket, roleArn: Arn)
