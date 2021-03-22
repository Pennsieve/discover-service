// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.downloads

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.ByteString

import com.blackfynn.test.AwaitableImplicits
import com.blackfynn.discover.TestUtilities._
import com.blackfynn.discover.clients.{ MockS3StreamClient, TestFile }
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration.DurationInt

import java.nio.file.{ Files, Path, Paths }

class ZipStreamSpec
    extends WordSpec
    with Matchers
    with AwaitableImplicits
    with ScalaFutures
    with TempDirectoryFixture {

  implicit private val system: ActorSystem = ActorSystem("discover-service")
  implicit private val materializer: ActorMaterializer = ActorMaterializer()
  implicit private val executionContext: ExecutionContext = system.dispatcher

  "zip stream" should {
    "create a zip archive from a collection of input streams" in withTempDirectory {
      tempDir: Path =>
        {

          val streamClient = new MockS3StreamClient()
          val testFiles = List(
            TestFile(1000, tempDir, "1.pdf", "test/1.pdf"),
            TestFile(72000, tempDir, "2.txt", "test/2.txt"),
            TestFile(1289, tempDir, "3.txt", "test/nested/3.txt"),
            TestFile(1289, tempDir, "4.txt", "test/nested/4.txt")
          )
          testFiles.foreach(_.generate)

          val outPath = tempDir.resolve("output.zip")

          Source(testFiles.map(_.zipSource))
            .via(ZipStream())
            .runWith(FileIO.toPath(outPath))
            .awaitFinite(60.seconds)

          unzipArchive(outPath.toString, tempDir.toString)
          TestFile.sourceAndDestAreEqual(testFiles) shouldBe true
        }
    }
  }

}
