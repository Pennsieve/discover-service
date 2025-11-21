// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

import com.dimafeng.testcontainers.{ Container, GenericContainer }
import com.pennsieve.discover.testcontainers.S3DockerContainer.{
  accessKey,
  secretKey
}
import com.pennsieve.test.{ DockerContainer, StackedDockerContainer }
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import java.net.URI

object S3DockerContainer {
  val minioTag = "RELEASE.2022-10-20T00-55-09Z"
  val accessKey: String = "access_key"
  val secretKey: String = "access_secret"
  val port: Int = 9000
}

trait S3DockerContainer extends StackedDockerContainer {
  lazy val s3Container: S3DockerContainerImpl =
    DockerContainers.s3DockerContainerImpl
  override def stackedContainers: List[Container] =
    s3Container :: super.stackedContainers
}

final class S3DockerContainerImpl
    extends DockerContainer(
      dockerImage = s"minio/minio:${S3DockerContainer.minioTag}",
      exposedPorts = Seq(S3DockerContainer.port),
      env = Map(
        "MINIO_ROOT_USER" -> S3DockerContainer.accessKey,
        "MINIO_ROOT_PASSWORD" -> S3DockerContainer.secretKey
      ),
      waitStrategy = Some(new HttpWaitStrategy().forPath("/minio/health/live")),
      command = Seq("server", "/tmp")
    ) {

  override def mappedPort(): Int = super.mappedPort(S3DockerContainer.port)

  def s3Endpoint: String = s"http://localhost:${mappedPort()}"

  // alpakka-s3 v1.0 can only be configured via Typesafe config passed to the
  // actor system, or as S3Settings that are attached to every graph
  override def config: Config =
    ConfigFactory
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
      .withValue(
        "alpakka.s3.access-style",
        ConfigValueFactory.fromAnyRef("path")
      )

  def s3Client(httpClient: SdkHttpClient): S3Client =
    S3Client
      .builder()
      .region(Region.US_EAST_1)
      .credentialsProvider(
        StaticCredentialsProvider
          .create(AwsBasicCredentials.create(accessKey, secretKey))
      )
      .httpClient(httpClient)
      .endpointOverride(new URI(s3Endpoint))
      .build()
}
