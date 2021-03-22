// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import scala.collection.immutable.IndexedSeq
import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed trait PennsieveSchemaVersion
    extends EnumEntry
    with Ordered[PennsieveSchemaVersion] {

  def compare(that: PennsieveSchemaVersion) =
    PennsieveSchemaVersion.indexOf(this) - PennsieveSchemaVersion.indexOf(that)
}

object PennsieveSchemaVersion
    extends Enum[PennsieveSchemaVersion]
    with CirceEnum[PennsieveSchemaVersion] {

  val values: IndexedSeq[PennsieveSchemaVersion] = findValues
  val latest: PennsieveSchemaVersion = values.last

  case object `1.0` extends PennsieveSchemaVersion
  case object `2.0` extends PennsieveSchemaVersion
  case object `3.0` extends PennsieveSchemaVersion
  case object `4.0` extends PennsieveSchemaVersion

}
