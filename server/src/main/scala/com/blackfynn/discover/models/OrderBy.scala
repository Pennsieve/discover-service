// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed abstract class OrderBy(override val entryName: String) extends EnumEntry

object OrderBy extends Enum[OrderBy] with CirceEnum[OrderBy] {
  val values = findValues

  val default: OrderBy = Relevance

  case object Name extends OrderBy("name")
  case object Date extends OrderBy("date")
  case object Relevance extends OrderBy("relevance")
  case object Size extends OrderBy("size")
}
