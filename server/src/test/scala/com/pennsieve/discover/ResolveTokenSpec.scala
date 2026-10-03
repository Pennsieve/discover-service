// Copyright (c) 2026 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import io.circe.Json
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.Base64

object ResolveTokenSpec {
  val key: String = "k" * 40

  private def b64(json: Json): String =
    Base64.getUrlEncoder.withoutPadding
      .encodeToString(json.noSpaces.getBytes(UTF_8))

  /** A token as download-service (golang-jwt) mints it. */
  def mint(
    now: Instant,
    key: String = key,
    alg: String = "HS256",
    iss: String = ResolveToken.Issuer,
    aud: Json = Json.arr(Json.fromString(ResolveToken.Audience)),
    lifetime: Long = 60
  ): String = {
    val header = b64(
      Json.obj("alg" -> Json.fromString(alg), "typ" -> Json.fromString("JWT"))
    )
    val t = now.getEpochSecond
    val payload = b64(
      Json.obj(
        "iss" -> Json.fromString(iss),
        "aud" -> aud,
        "iat" -> Json.fromLong(t),
        "nbf" -> Json.fromLong(t),
        "exp" -> Json.fromLong(t + lifetime)
      )
    )
    s"$header.$payload.${ResolveToken.sign(s"$header.$payload", key)}"
  }
}

class ResolveTokenSpec extends AnyWordSpec with Matchers {
  import ResolveTokenSpec._

  val now: Instant = Instant.parse("2026-10-02T12:00:00Z")

  "ResolveToken.verify" should {
    "accept a token download-service minted" in {
      ResolveToken.verify(mint(now), key, now) shouldBe Right(())
    }

    "accept a single-string audience" in {
      ResolveToken.verify(
        mint(now, aud = Json.fromString(ResolveToken.Audience)),
        key,
        now
      ) shouldBe Right(())
    }

    "refuse a token signed with another key, such as the platform JWT key" in {
      ResolveToken.verify(mint(now, key = "p" * 40), key, now) shouldBe Left(
        "bad signature"
      )
    }

    "refuse other issuers, audiences and algorithms" in {
      ResolveToken.verify(mint(now, iss = "someone"), key, now) shouldBe Left(
        "wrong issuer"
      )
      ResolveToken.verify(
        mint(now, aud = Json.arr(Json.fromString("pennsieve"))),
        key,
        now
      ) shouldBe Left("wrong audience")
      ResolveToken.verify(mint(now, alg = "none"), key, now) shouldBe Left(
        "unexpected algorithm"
      )
    }

    "refuse an expired or long-lived token" in {
      ResolveToken.verify(mint(now), key, now.plusSeconds(200)) shouldBe Left(
        "expired"
      )
      ResolveToken.verify(mint(now, lifetime = 3600), key, now) shouldBe Left(
        "lifetime too long"
      )
    }

    "refuse everything while the key isn't configured" in {
      ResolveToken.verify(mint(now, key = "short"), "short", now) shouldBe Left(
        "resolve key not configured"
      )
    }

    "refuse malformed tokens" in {
      ResolveToken.verify("not-a-token", key, now) shouldBe Left(
        "malformed token"
      )
      ResolveToken.verify("a.b.c", key, now).isLeft shouldBe true
    }
  }

  "ResolveToken.bearer" should {
    "take the token from a Bearer value" in {
      ResolveToken.bearer("Bearer abc") shouldBe Some("abc")
      ResolveToken.bearer("bearer  abc ") shouldBe Some("abc")
      ResolveToken.bearer("Basic abc") shouldBe None
      ResolveToken.bearer("Bearer") shouldBe None
    }
  }
}
