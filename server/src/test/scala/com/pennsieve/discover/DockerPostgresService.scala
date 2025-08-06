// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import java.net.ServerSocket
import java.sql.DriverManager

import com.whisk.docker._

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try

object TestPostgresConfiguration {
  val advertisedPort: Int = 5432

  def freshPostgresConfiguration: PostgresConfiguration = {

    //TODO: Have DockerKit select the exposed port for us
    val exposedPort: Int = {
      // ServerSocket will find an available port given port "0"
      val socket = new ServerSocket(0)
      val port = socket.getLocalPort
      socket.close()
      port
    }

    PostgresConfiguration(
      host = "localhost",
      port = exposedPort,
      database = "postgres",
      user = "postgres",
      password = "password",
      useSSL = false
    )
  }
}

trait DockerPostgresService extends DockerKit {

  val postgresConfiguration: PostgresConfiguration =
    TestPostgresConfiguration.freshPostgresConfiguration

  val postgresContainer: DockerContainer =
    DockerContainer("postgres:16.8")
      .withPorts(
        (
          TestPostgresConfiguration.advertisedPort,
          Some(postgresConfiguration.port)
        )
      )
      .withEnv(
        s"POSTGRES_USER=${postgresConfiguration.user}",
        s"POSTGRES_PASSWORD=${postgresConfiguration.password}"
      )
      .withReadyChecker(
        new PostgresReadyChecker(postgresConfiguration)
          .looped(30, 1.second)
      )

  abstract override def dockerContainers: List[DockerContainer] =
    postgresContainer :: super.dockerContainers

}

class PostgresReadyChecker(configuration: PostgresConfiguration)
    extends DockerReadyChecker {

  override def apply(
    container: DockerContainerState
  )(implicit
    docker: DockerCommandExecutor,
    executionContext: ExecutionContext
  ): Future[Boolean] = {
    container
      .getPorts()
      .map { ports =>
        val ready: Try[Boolean] = Try {
          Class.forName(configuration.driver)

          Option(
            DriverManager.getConnection(
              configuration
                .copy(host = docker.host, port = configuration.port)
                .jdbcURL,
              configuration.user,
              configuration.password
            )
          ).map(_.close).isDefined
        }

        ready.getOrElse(false)
      }
  }
}
