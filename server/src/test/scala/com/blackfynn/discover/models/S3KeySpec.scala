// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.syntax._
import io.circe.parser.decode
import io.circe.{ Decoder, Encoder, HCursor, Json }

import org.scalatest.{ Matchers, WordSpec }

class S3KeySpec extends WordSpec with Matchers {

  "S3 dataset key" should {

    "build from id" in {
      S3Key.Dataset(0) shouldBe S3Key.Dataset("versioned/0/")
    }

    "join with a path to make a file key" in {
      S3Key.Dataset(0) / "files/test.txt" shouldBe S3Key.File(
        "versioned/0/files/test.txt"
      )
    }

    "strip leading slashes when joining" in {
      S3Key.Dataset(0) / "/files/test.txt" shouldBe S3Key.File(
        "versioned/0/files/test.txt"
      )
    }

    "strip trailing slashes when joining" in {
      S3Key.Dataset(0) / "files/test.txt/" shouldBe S3Key.File(
        "versioned/0/files/test.txt"
      )
    }

    "be converted to a string" in {
      S3Key.Dataset(0).toString shouldBe "versioned/0/"
    }

    "be encoded correctly" in {
      S3Key.Dataset(0).asJson shouldBe Json.fromString("versioned/0/")
    }
    "be decoded correctly" in {
      decode[S3Key.Dataset]("\"versioned/0/\"") shouldBe Right(S3Key.Dataset(0))
    }

  }

  "S3 file key" should {
    "be converted to a string" in {
      S3Key
        .File("versioned/0/test.txt")
        .toString shouldBe "versioned/0/test.txt"
    }

    "be encoded correctly" in {
      S3Key.File("versioned/0/test.txt").asJson shouldBe Json.fromString(
        "versioned/0/test.txt"
      )
    }
    "be decoded correctly" in {
      decode[S3Key.File]("\"versioned/0/test.txt\"") shouldBe Right(
        S3Key.File("versioned/0/test.txt")
      )
    }
    "remove version prefix" in {
      S3Key
        .File("versioned/0/test.txt")
        .removeDatasetPrefix(S3Key.Dataset(0)) shouldBe "test.txt"
    }
  }

  "S3 revision key" should {

    "be built correctly" in {
      S3Key.Revision(0, 1).asJson shouldBe Json.fromString(
        "versioned/0/revisions/1/"
      )
    }
  }

}
