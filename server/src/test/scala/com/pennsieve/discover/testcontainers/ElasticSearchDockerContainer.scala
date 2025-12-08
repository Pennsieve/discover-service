// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

import com.dimafeng.testcontainers.Container
import com.pennsieve.discover.ElasticSearchConfiguration
import com.pennsieve.test.{ DockerContainer, StackedDockerContainer }
import org.testcontainers.containers.wait.strategy.{
  HttpWaitStrategy,
  LogMessageWaitStrategy
}

object ElasticSearchDockerContainer {
  val elasticSearchVersion = "7.10.1"
  val port: Int = 9200
  // copied from testcontainer's own ElasticSearch module
  // regex that
  //   matches 8.3 JSON logging with started message and some follow up content within the message field
  //   matches 8.0 JSON logging with no whitespace between message field and content
  //   matches 7.x JSON logging with whitespace between message field and content
  //   matches 6.x text logging with node name in brackets and just a 'started' message till the end of the line
  val regex = ".*(\"message\":\\s?\"started[\\s?|\"].*|] started\n$)"
}

trait ElasticSearchDockerContainer extends StackedDockerContainer {
  lazy val elasticSearchContainer: ElasticSearchDockerContainerImpl =
    DockerContainers.elasticSearchDockerContainerImpl
  override def stackedContainers: List[Container] =
    elasticSearchContainer :: super.stackedContainers

}

final class ElasticSearchDockerContainerImpl
    extends DockerContainer(
      dockerImage =
        s"docker.elastic.co/elasticsearch/elasticsearch-oss:${ElasticSearchDockerContainer.elasticSearchVersion}",
      exposedPorts = Seq(ElasticSearchDockerContainer.port),
      env = Map("discovery.type" -> "single-node"),
      waitStrategy = Some(
        new LogMessageWaitStrategy()
          .withRegEx(ElasticSearchDockerContainer.regex)
          .withTimes(1)
      )
    ) {

  override def mappedPort(): Int =
    super.mappedPort(ElasticSearchDockerContainer.port)

  def elasticSearchConfiguration: ElasticSearchConfiguration =
    ElasticSearchConfiguration(host = "http://localhost", port = mappedPort())
}
