// Copyright (c) 2026 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import io.circe.Json
import io.circe.parser.parse

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
  * Verifies the tokens download-service sends to the download-resolve route
  * (download-service docs/public-downloads.md).
  *
  * They are HS256 JWTs signed with a key that only download-service and this
  * route share: not the platform JWT key, which can mint user and service
  * claims every service accepts. The issuer and audience bind a token to
  * this one use, and tokens are minted for a minute.
  */
object ResolveToken {
  val Issuer = "download-service"
  val Audience = "discover-resolve"

  /** download-service mints tokens for 60 s; anything longer is refused. */
  val MaxLifetimeSeconds: Long = 120

  /** Clock skew allowed on exp and nbf. */
  val LeewaySeconds: Long = 30

  /** Keys shorter than this mean the route isn't configured. */
  val MinKeyLength = 32

  def verify(token: String, key: String, now: Instant): Either[String, Unit] =
    if (key.length < MinKeyLength) Left("resolve key not configured")
    else
      token.split('.') match {
        case Array(header, payload, signature) =>
          for {
            h <- decode(header)
            _ <- check(
              h.hcursor.get[String]("alg").toOption.contains("HS256"),
              "unexpected algorithm"
            )
            _ <- check(
              MessageDigest.isEqual(
                sign(s"$header.$payload", key).getBytes(UTF_8),
                signature.getBytes(UTF_8)
              ),
              "bad signature"
            )
            claims <- decode(payload)
            c = claims.hcursor
            _ <- check(
              c.get[String]("iss").toOption.contains(Issuer),
              "wrong issuer"
            )
            _ <- check(audiences(claims).contains(Audience), "wrong audience")
            exp <- c.get[Long]("exp").left.map(_ => "missing exp")
            iat <- c.get[Long]("iat").left.map(_ => "missing iat")
            nbf = c.get[Long]("nbf").getOrElse(iat)
            _ <- check(now.getEpochSecond <= exp + LeewaySeconds, "expired")
            _ <- check(nbf <= now.getEpochSecond + LeewaySeconds, "not yet valid")
            _ <- check(exp - iat <= MaxLifetimeSeconds, "lifetime too long")
          } yield ()
        case _ => Left("malformed token")
      }

  /** The token from an "Authorization: Bearer …" value. */
  def bearer(value: String): Option[String] =
    value.trim.split("\\s+", 2) match {
      case Array(scheme, token) if scheme.equalsIgnoreCase("Bearer") =>
        Some(token.trim).filter(_.nonEmpty)
      case _ => None
    }

  /** HMAC-SHA256 of input, base64url without padding (as in a JWT). */
  def sign(input: String, key: String): String = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(key.getBytes(UTF_8), "HmacSHA256"))
    Base64.getUrlEncoder.withoutPadding
      .encodeToString(mac.doFinal(input.getBytes(UTF_8)))
  }

  private def decode(part: String): Either[String, Json] =
    for {
      bytes <- scala.util
        .Try(Base64.getUrlDecoder.decode(part))
        .toEither
        .left
        .map(_ => "malformed token")
      json <- parse(new String(bytes, UTF_8)).left.map(_ => "malformed token")
    } yield json

  /** aud is a string or an array of strings. */
  private def audiences(claims: Json): Seq[String] = {
    val aud = claims.hcursor.downField("aud")
    aud
      .as[String]
      .map(Seq(_))
      .orElse(aud.as[Seq[String]])
      .getOrElse(Seq.empty)
  }

  private def check(ok: Boolean, reason: String): Either[String, Unit] =
    if (ok) Right(()) else Left(reason)
}
