// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import com.pennsieve.test.AwaitableImplicits
import com.whisk.docker._

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._

trait DockerS3Service extends DockerKit with AwaitableImplicits {

  val minioTag = "RELEASE.2022-10-20T00-55-09Z"
  val accessKey: String = "access_key"
  val secretKey: String = "access_secret"
  val minioExposedPort: Int = 9000

  val s3Container: DockerContainer = DockerContainer(s"minio/minio:$minioTag")
    .withPorts(minioExposedPort -> None)
    .withEnv(s"MINIO_ACCESS_KEY=$accessKey", s"MINIO_SECRET_KEY=$secretKey")
    .withCommand("server", "/tmp")
    .withReadyChecker(
      DockerReadyChecker
        .HttpResponseCode(port = minioExposedPort, path = "/minio/health/live")
        .within(100.millis)
        .looped(20, 1250.millis)
    )

  lazy val s3ContainerPort: Int =
    s3Container.getPorts().map(_(minioExposedPort)).awaitFinite()

  lazy val s3Endpoint: String =
    s"http://localhost:$s3ContainerPort"

  abstract override def dockerContainers: List[DockerContainer] =
    s3Container :: super.dockerContainers

}
