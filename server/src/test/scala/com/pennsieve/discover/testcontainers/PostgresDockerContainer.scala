// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.testcontainers

import com.dimafeng.testcontainers.Container
import com.pennsieve.discover.PostgresConfiguration
import com.pennsieve.test.{ DockerContainer, StackedDockerContainer }
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy

import java.time.Duration

object PostgresDockerContainer {
  val port: Int = 5432
  val host = "localhost"
  val database = "postgres"
  val user = "postgres"
  val password = "password"
  val useSSL = false

  val version = "16.8"
}

trait PostgresDockerContainer extends StackedDockerContainer {
  val postgresContainer: PostgresDockerContainerImpl =
    DockerContainers.postgresContainer
  override def stackedContainers: List[Container] =
    postgresContainer :: super.stackedContainers

}

final class PostgresDockerContainerImpl
    extends DockerContainer(
      dockerImage = s"postgres:${PostgresDockerContainer.version}",
      exposedPorts = Seq(PostgresDockerContainer.port),
      env = Map(
        "POSTGRES_USER" -> PostgresDockerContainer.user,
        "POSTGRES_PASSWORD" -> PostgresDockerContainer.password
      ),
      // Disable data durability to speed up tests. See
      // https://kubuszok.com/2018/speed-up-things-in-scalac-and-sbt/
      command = Seq(
        "-c",
        "fsync=off",
        "-c",
        "synchronous_commit=off",
        "-c",
        "full_page_writes=off"
      ),
      waitStrategy = Some(
        new LogMessageWaitStrategy()
          .withRegEx(".*database system is ready to accept connections.*\\s")
          .withStartupTimeout(Duration.ofMinutes(1))
      )
    ) {

  override def mappedPort(): Int =
    super.mappedPort(PostgresDockerContainer.port)

  def postgresConfiguration: PostgresConfiguration = PostgresConfiguration(
    host = PostgresDockerContainer.host,
    port = mappedPort(),
    database = PostgresDockerContainer.database,
    user = PostgresDockerContainer.user,
    password = PostgresDockerContainer.password,
    useSSL = PostgresDockerContainer.useSSL
  )
}
