// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

import com.dimafeng.testcontainers.Container
import com.pennsieve.discover.testcontainers.MockServerDockerContainer.{
  accessKey,
  healthCheckPath,
  secretKey
}
import com.pennsieve.test.{ DockerContainer, StackedDockerContainer }
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import org.mockserver.client.MockServerClient
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

object MockServerDockerContainer {
  val accessKey: String = "access_key"
  val secretKey: String = "access_secret"
  val port: Int = 1080
  val healthCheckPath = "/mockServer/healthCheck"
}

trait MockServerDockerContainer extends StackedDockerContainer {
  val mockServerContainer: MockServerDockerContainerImpl =
    DockerContainers.mockServerContainer

  val accessKey: String = MockServerDockerContainer.accessKey
  val secretKey: String = MockServerDockerContainer.secretKey

  override def stackedContainers: List[Container] =
    mockServerContainer :: super.stackedContainers
}

final class MockServerDockerContainerImpl
    extends DockerContainer(
      dockerImage = "mockserver/mockserver:5.14.0",
      exposedPorts = Seq(MockServerDockerContainer.port),
      env = Map("MOCKSERVER_LIVENESS_HTTP_GET_PATH" -> healthCheckPath),
      waitStrategy = Some(new HttpWaitStrategy().forPath(healthCheckPath))
    ) {

  override def mappedPort(): Int =
    super.mappedPort(MockServerDockerContainer.port)

  def mockServerEndpoint: String =
    s"http://localhost:${mappedPort()}"

  // alpakka-s3 v1.0 can only be configured via Typesafe config passed to the
  // actor system, or as S3Settings that are attached to every graph
  override def config: Config =
    ConfigFactory
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
      .withValue(
        "alpakka.s3.access-style",
        ConfigValueFactory.fromAnyRef("path")
      )

  def mockServerClient: MockServerClient = {
    new MockServerClient("localhost", mappedPort())
  }
}
