// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

import com.pennsieve.test.DockerContainer
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy

object ElasticSearchDockerContainer {
  val elasticSearchVersion = "7.10.1"
  val port: Int = 9200
}

final class ElasticSearchDockerContainerImpl
    extends DockerContainer(
      dockerImage =
        s"docker.elastic.co/elasticsearch/elasticsearch-oss:${ElasticSearchDockerContainer.elasticSearchVersion}",
      exposedPorts = Seq(ElasticSearchDockerContainer.port),
      env = Map("discovery.type" -> "single-node"),
      waitStrategy = Some(new HttpWaitStrategy().forPath("/_cat/health"))
    ) {

  override def mappedPort(): Int =
    super.mappedPort(ElasticSearchDockerContainer.port)
}
