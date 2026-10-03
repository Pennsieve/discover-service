// Copyright (c) 2026 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import akka.http.scaladsl.model.headers.{
  Authorization,
  OAuth2BearerToken,
  RawHeader
}
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpMethods,
  HttpRequest,
  StatusCodes
}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.pennsieve.discover._
import com.pennsieve.discover.clients.MockAuthorizationClient
import com.pennsieve.discover.models.PublicDatasetVersion
import com.pennsieve.discover.server.definitions.DownloadResolveResponse
import com.pennsieve.models.{ FileType, PublishStatus }
import io.circe.Json
import io.circe.parser.decode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class ResolveHandlerSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with ServiceSpecHarness {

  def resolvePorts: Ports =
    ports.copy(
      config = ports.config.copy(
        downloadResolve =
          DownloadResolveConfiguration(Some(ResolveTokenSpec.key))
      )
    )

  def routes(p: Ports = resolvePorts): Route =
    Route.seal(ResolveHandler.routes(p))

  def serviceToken: Authorization =
    Authorization(OAuth2BearerToken(ResolveTokenSpec.mint(Instant.now())))

  def resolve(
    datasetId: Int,
    version: String,
    body: Json = Json.obj("paths" -> Json.arr(Json.fromString(""))),
    path: String = ""
  ): HttpRequest =
    HttpRequest(
      method = HttpMethods.POST,
      uri = s"$path/datasets/$datasetId/versions/$version/download-resolve",
      entity = HttpEntity(ContentTypes.`application/json`, body.noSpaces)
    )

  def forwarded(header: Authorization): RawHeader =
    RawHeader(ResolveHandler.UserAuthorizationHeader, header.value)

  def parsed(body: String): DownloadResolveResponse =
    decode[DownloadResolveResponse](body).fold(e => fail(e.toString), identity)

  def publishedVersion(
    status: PublishStatus = PublishStatus.PublishSucceeded
  ): PublicDatasetVersion =
    TestUtilities.createDatasetV1(ports.db)(status = status, migrated = true)

  def addFile(
    v: PublicDatasetVersion,
    path: String,
    size: Long = 100,
    s3Version: Option[String] = Some(TestUtilities.defaultS3VersionId)
  ) =
    TestUtilities.createFileVersion(ports.db)(
      v,
      path,
      FileType.Text,
      size = size,
      s3Version = s3Version
    )

  def mockAuth: MockAuthorizationClient =
    ports.authorizationClient.asInstanceOf[MockAuthorizationClient]

  "the resolve route" should {
    "refuse requests without a resolve token" in {
      val v = publishedVersion()
      resolve(v.datasetId, v.version.toString) ~> routes() ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "refuse a platform JWT, even a valid one" in {
      val v = publishedVersion()
      resolve(v.datasetId, v.version.toString) ~> addHeader(
        mockAuth.authorizedHeader
      ) ~> routes() ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "refuse everything while the resolve key isn't configured" in {
      val v = publishedVersion()
      val unconfigured = ports.copy(
        config = ports.config
          .copy(downloadResolve = DownloadResolveConfiguration(None))
      )
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes(unconfigured) ~> check {
        status shouldEqual StatusCodes.Unauthorized
      }
    }

    "not be served under /public" in {
      val v = publishedVersion()
      resolve(v.datasetId, v.version.toString, path = "/public") ~> addHeader(
        serviceToken
      ) ~> routes() ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "resolve a whole version, with totals and object versions" in {
      val v = publishedVersion()
      val a = addFile(v, "A/a.txt", size = 10)
      val b = addFile(v, "b.txt", size = 20)

      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes() ~> check {
        status shouldEqual StatusCodes.OK
        val r = parsed(responseAs[String])
        r.decision shouldBe "allowed"
        r.version.number shouldBe v.version
        r.version.size shouldBe 30
        r.version.fileCount shouldBe 2
        r.version.legacy shouldBe false
        r.files.map(_.key) shouldBe Vector(a.s3Key.value, b.s3Key.value).sorted
        r.files.map(_.bucket).toSet shouldBe Set(v.s3Bucket.value)
        r.files.flatMap(_.s3Version).toSet shouldBe Set(
          TestUtilities.defaultS3VersionId
        )
        r.files.find(_.fileName == "a.txt").map(_.path) shouldBe Some(
          Vector("A")
        )
        r.next shouldBe None
      }
    }

    "resolve the latest version" in {
      val v = publishedVersion()
      addFile(v, "a.txt")
      resolve(v.datasetId, "latest") ~> addHeader(serviceToken) ~>
        routes() ~> check {
        val r = parsed(responseAs[String])
        r.decision shouldBe "allowed"
        r.version.number shouldBe v.version
      }
    }

    "page through a selection" in {
      val v = publishedVersion()
      addFile(v, "a.txt")
      addFile(v, "b.txt")
      addFile(v, "c.txt")
      val page1 = Json.obj(
        "paths" -> Json.arr(Json.fromString("")),
        "limit" -> Json.fromInt(2)
      )

      var next: Option[String] = None
      resolve(v.datasetId, v.version.toString, page1) ~> addHeader(
        serviceToken
      ) ~> routes() ~> check {
        val r = parsed(responseAs[String])
        r.files.map(_.fileName) shouldBe Vector("a.txt", "b.txt")
        r.version.fileCount shouldBe 3
        next = r.next
      }
      next should not be empty

      val page2 = page1.deepMerge(Json.obj("cursor" -> Json.fromString(next.get)))
      resolve(v.datasetId, v.version.toString, page2) ~> addHeader(
        serviceToken
      ) ~> routes() ~> check {
        val r = parsed(responseAs[String])
        r.files.map(_.fileName) shouldBe Vector("c.txt")
        r.next shouldBe None
      }
    }

    "report files without an object version instead of returning them" in {
      val v = publishedVersion()
      addFile(v, "a.txt")
      addFile(v, "old.txt", s3Version = Some(ResolveHandler.MissingVersion))
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes() ~> check {
        val r = parsed(responseAs[String])
        r.files.map(_.fileName) shouldBe Vector("a.txt")
        r.skipped.map(_.fileName) shouldBe Vector("old.txt")
        r.skipped.map(_.reason) shouldBe Vector("no_version")
      }
    }

    "ask for sign-in for an embargoed version without a user, with no files" in {
      val v = publishedVersion(PublishStatus.EmbargoSucceeded)
      addFile(v, "a.txt")
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes() ~> check {
        val r = parsed(responseAs[String])
        r.decision shouldBe "sign_in"
        r.version.embargoed shouldBe true
        r.files shouldBe empty
      }
    }

    "allow an embargoed version for a user with access, and forbid others" in {
      val v = publishedVersion(PublishStatus.EmbargoSucceeded)
      addFile(v, "a.txt")
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        addHeader(forwarded(mockAuth.authorizedHeader)) ~> routes() ~> check {
        val r = parsed(responseAs[String])
        r.decision shouldBe "allowed"
        r.files.map(_.fileName) shouldBe Vector("a.txt")
      }
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        addHeader(forwarded(mockAuth.forbiddenHeader)) ~> routes() ~> check {
        val r = parsed(responseAs[String])
        r.decision shouldBe "forbidden"
        r.files shouldBe empty
      }
    }

    "never use the request's own Authorization as the user's credential" in {
      // The resolve token is the only Authorization here: an embargoed
      // version still needs a forwarded user credential.
      val v = publishedVersion(PublishStatus.EmbargoSucceeded)
      addFile(v, "a.txt")
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes() ~> check {
        parsed(responseAs[String]).decision shouldBe "sign_in"
      }
    }

    "answer not_found and unpublished" in {
      resolve(999999, "1") ~> addHeader(serviceToken) ~> routes() ~> check {
        parsed(responseAs[String]).decision shouldBe "not_found"
      }
      val v = publishedVersion(PublishStatus.Unpublished)
      resolve(v.datasetId, v.version.toString) ~> addHeader(serviceToken) ~>
        routes() ~> check {
        parsed(responseAs[String]).decision shouldBe "unpublished"
      }
      resolve(v.datasetId, "not-a-version") ~> addHeader(serviceToken) ~>
        routes() ~> check {
        parsed(responseAs[String]).decision shouldBe "not_found"
      }
    }

    "refuse a cursor it didn't issue" in {
      val v = publishedVersion()
      val body = Json.obj(
        "paths" -> Json.arr(Json.fromString("")),
        "cursor" -> Json.fromString("bogus")
      )
      resolve(v.datasetId, v.version.toString, body) ~> addHeader(
        serviceToken
      ) ~> routes() ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }
  }
}
