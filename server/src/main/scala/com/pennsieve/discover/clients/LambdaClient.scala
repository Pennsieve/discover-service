// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.clients

import akka.actor.ActorSystem
import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.scaladsl.{ Sink, Source }
import com.pennsieve.discover.LambdaException
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.lambda.model.{
  InvokeRequest,
  InvokeResponse
}
import software.amazon.awssdk.services.lambda.LambdaAsyncClient

import scala.concurrent.{ ExecutionContext, Future }

trait LambdaClient {

  def runS3Clean(
    s3KeyPrefix: String,
    publishBucket: String,
    embargoBucket: String
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[InvokeResponse]

}

class AlpakkaLambdaClient(
  s3CleanFunction: String,
  region: Region,
  parallelism: Int = 1
) extends LambdaClient
    with StrictLogging {

  private lazy val lambdaClient: LambdaAsyncClient = LambdaAsyncClient
    .builder()
    .httpClientBuilder(NettyNioAsyncHttpClient.builder())
    .credentialsProvider(DefaultCredentialsProvider.create())
    .region(region)
    .build()

  def runS3Clean(
    s3KeyPrefix: String,
    publishBucket: String,
    embargoBucket: String
  )(implicit
    system: ActorSystem,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    val lambdaRequest = InvokeRequest
      .builder()
      .functionName(s3CleanFunction)
      .payload(
        SdkBytes
          .fromUtf8String(
            Map(
              "s3_key_prefix" -> s3KeyPrefix,
              "publish_bucket" -> publishBucket,
              "embargo_bucket" -> embargoBucket
            ).asJson.noSpaces
          )
      )
      .build()
    Source
      .single(lambdaRequest)
      .via(AwsLambdaFlow(parallelism)(lambdaClient))
      .runWith(Sink.head)
      .flatMap {
        case response: InvokeResponse =>
          if (response.statusCode() == 200) {
            Future.successful(response)
          } else {
            Future.failed(
              LambdaException(s3KeyPrefix, publishBucket, embargoBucket)
            )
          }
        case _ =>
          Future.failed(
            LambdaException(s3KeyPrefix, publishBucket, embargoBucket)
          )
      }
  }

}
