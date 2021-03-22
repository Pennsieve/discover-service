// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed abstract class OrderDirection(override val entryName: String)
    extends EnumEntry

object OrderDirection
    extends Enum[OrderDirection]
    with CirceEnum[OrderDirection] {
  val values = findValues

  val default: OrderDirection = Descending

  case object Ascending extends OrderDirection("asc")
  case object Descending extends OrderDirection("desc")
}
