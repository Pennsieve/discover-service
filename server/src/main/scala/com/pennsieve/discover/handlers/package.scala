// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.handlers

import cats.implicits._
import com.pennsieve.discover.BadQueryParameter

import scala.concurrent.Future

package object param {

  /**
    * Parse an optional string query parameter to some typeful value.
    */
  def parse[A](
    param: Option[String],
    parser: String => A,
    default: A
  ): Future[A] =
    param
      .map(
        value =>
          Either
            .catchNonFatal(parser(value))
            .leftMap(BadQueryParameter(_))
      )
      .getOrElse(Right(default))
      .fold(Future.failed, Future.successful)
}
