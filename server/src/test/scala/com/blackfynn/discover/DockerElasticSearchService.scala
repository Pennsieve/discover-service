// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover

import com.blackfynn.test.AwaitableImplicits
import com.whisk.docker._

import scala.concurrent.duration._

trait DockerElasticSearchService extends DockerKit with AwaitableImplicits {

  val elasticSearchVersion = "6.7.2"
  val advertisedPort: Int = 9200

  lazy val elasticSearchConfiguration: ElasticSearchConfiguration =
    ElasticSearchConfiguration(
      host = "http://localhost",
      port = elasticHttpPort
    )

  val elasticSearchContainer: DockerContainer =
    DockerContainer(
      s"docker.elastic.co/elasticsearch/elasticsearch-oss:$elasticSearchVersion"
    ).withPorts(advertisedPort -> None)
      .withEnv("discovery.type=single-node")
      .withReadyChecker(
        DockerReadyChecker
          .HttpResponseCode(port = advertisedPort, path = "/_cat/health")
          .looped(30, 1.second)
      )

  lazy val elasticHttpPort: Int =
    elasticSearchContainer
      .getPorts()
      .map(_(advertisedPort))
      .awaitFinite()

  abstract override def dockerContainers: List[DockerContainer] =
    elasticSearchContainer :: super.dockerContainers
}
