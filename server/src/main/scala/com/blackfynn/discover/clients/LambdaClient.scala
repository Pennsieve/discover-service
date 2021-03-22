// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.clients

import akka.stream.ActorMaterializer
import akka.stream.alpakka.awslambda.scaladsl.AwsLambdaFlow
import akka.stream.scaladsl.{Sink, Source}
import com.blackfynn.discover.LambdaException
import com.blackfynn.discover.models.S3Key.Dataset
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.lambda.model.{InvokeRequest, InvokeResponse}
import software.amazon.awssdk.services.lambda.LambdaAsyncClient

import scala.concurrent.{ExecutionContext, Future}

trait LambdaClient {

  def runS3Clean(
                  datasetId: Int
  )(implicit
    materializer: ActorMaterializer,
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
    datasetId: Int
  )(implicit
    materializer: ActorMaterializer,
    ec: ExecutionContext
  ): Future[InvokeResponse] = {
    val lambdaRequest = InvokeRequest
      .builder()
      .functionName(s3CleanFunction)
      .payload(
        SdkBytes
          .fromUtf8String(Map("s3_key_prefix" -> Dataset(datasetId).toString, "version" -> "all", "dataset_id" -> datasetId.toString, "s3_versioned_files_key" -> "")
          ))
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
            Future.failed(LambdaException(s3KeyPrefix))
          }
        case _ => Future.failed(LambdaException(s3KeyPrefix))
      }
  }

}
