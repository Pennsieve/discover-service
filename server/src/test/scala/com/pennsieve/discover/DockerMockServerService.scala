// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.test.AwaitableImplicits
import com.whisk.docker._
import org.mockserver.client.MockServerClient

import scala.concurrent.duration._

trait DockerMockServerService extends DockerKit with AwaitableImplicits {

  val accessKey: String = "access_key"
  val secretKey: String = "access_secret"
  val mockServerExposedPort: Int = 1080
  val healthCheckPath = "/mockServer/healthCheck"

  val mockServerContainer: DockerContainer = DockerContainer(
    s"mockserver/mockserver:5.14.0"
  ).withPorts(mockServerExposedPort -> None)
    .withEnv(s"MOCKSERVER_LIVENESS_HTTP_GET_PATH=${healthCheckPath}")
    .withReadyChecker(
      DockerReadyChecker
        .HttpResponseCode(port = mockServerExposedPort, path = healthCheckPath)
        .within(100.millis)
        .looped(20, 1250.millis)
    )

  lazy val mockServerContainerPort: Int =
    mockServerContainer.getPorts().map(_(mockServerExposedPort)).awaitFinite()

  lazy val mockServerEndpoint: String =
    s"http://localhost:$mockServerContainerPort"

  abstract override def dockerContainers: List[DockerContainer] =
    mockServerContainer :: super.dockerContainers

  def mockServerClient: MockServerClient = {
    new MockServerClient("localhost", mockServerContainerPort)
  }

}
