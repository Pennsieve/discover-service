// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

/**
  * Shared singleton Docker containers.
  *
  * Containers are cached on this object so that the same container can be used
  * across multiple test suites by the PersistantDockerContainers trait.
  */
object DockerContainers {
  val postgresContainer: PostgresDockerContainerImpl =
    new PostgresDockerContainerImpl

  lazy val elasticSearchDockerContainerImpl: ElasticSearchDockerContainerImpl =
    new ElasticSearchDockerContainerImpl

  lazy val s3DockerContainerImpl: DiscoverServiceS3DockerContainerImpl =
    new DiscoverServiceS3DockerContainerImpl

  lazy val mockServerContainer: MockServerDockerContainerImpl =
    new MockServerDockerContainerImpl
}
