// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import io.circe.syntax._
import io.circe.parser.decode
import io.circe.Json
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class S3KeySpec extends AnyWordSpec with Matchers {

  "S3 version key" should {

    "build from id and version" in {
      S3Key.Version(0, 1) shouldBe S3Key.Version("0/1/")
    }

    "join with a path to make a file key" in {
      S3Key.Version(0, 1) / "files/test.txt" shouldBe S3Key.File(
        "0/1/files/test.txt"
      )
    }

    "strip leading slashes when joining" in {
      S3Key.Version(0, 1) / "/files/test.txt" shouldBe S3Key.File(
        "0/1/files/test.txt"
      )
    }

    "strip trailing slashes when joining" in {
      S3Key.Version(0, 1) / "files/test.txt/" shouldBe S3Key.File(
        "0/1/files/test.txt"
      )
    }

    "be converted to a string" in {
      S3Key.Version(0, 1).toString shouldBe "0/1/"
    }

    "be encoded correctly" in {
      S3Key.Version(0, 1).asJson shouldBe Json.fromString("0/1/")
    }
    "be decoded correctly" in {
      decode[S3Key.Version]("\"0/1/\"") shouldBe Right(S3Key.Version(0, 1))
    }

  }

  "S3 file key" should {
    "be converted to a string" in {
      S3Key.File("0/1/test.txt").toString shouldBe "0/1/test.txt"
    }

    "be encoded correctly" in {
      S3Key.File("0/1/test.txt").asJson shouldBe Json.fromString("0/1/test.txt")
    }
    "be decoded correctly" in {
      decode[S3Key.File]("\"0/1/test.txt\"") shouldBe Right(
        S3Key.File("0/1/test.txt")
      )
    }
    "remove version prefix" in {
      S3Key
        .File("0/1/test.txt")
        .removeVersionPrefix(S3Key.Version(0, 1)) shouldBe "test.txt"
    }
  }

  "S3 revision key" should {

    "be built correctly" in {
      S3Key.Revision(0, 1, 3).asJson shouldBe Json.fromString(
        "0/1/revisions/3/"
      )
    }
  }
}
