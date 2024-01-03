// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover

import java.util.Base64

package object db {
  object profile extends PostgresProfile

  // Our version of Postgres does not allow '+' or '/' in Ltree labels.
  // But both are valid Base64 alphabet characters, so we need to switch them out unambiguously.
  // The underscore is a legal ltree label character, but is not in the base 64 alphabet,
  // so we use it to replace the characters Postgres does not like.
  // (Later versions of Postgres apparently allow '-' in ltree labels, but not our version.)
  def pgSafeBase64(bytes: Array[Byte]): String = {
    Base64
      .getEncoder()
      .withoutPadding()
      .encodeToString(bytes)
      .replace("+", "_")
      .replace("/", "__")
  }
}
