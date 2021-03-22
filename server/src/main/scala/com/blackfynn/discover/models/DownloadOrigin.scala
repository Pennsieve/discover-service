// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import enumeratum.{ CirceEnum, Enum, EnumEntry }

sealed abstract class DownloadOrigin(override val entryName: String)
    extends EnumEntry

object DownloadOrigin
    extends Enum[DownloadOrigin]
    with CirceEnum[DownloadOrigin] {
  val values = findValues

  case object SPARC extends DownloadOrigin("SPARC")
  case object Discover extends DownloadOrigin("Discover")
  case object AllOrigins extends DownloadOrigin("AllOrigins")
  case object AWSRequesterPayer extends DownloadOrigin("AWSRequesterPayer")
  case object Unknown extends DownloadOrigin("Unknown")

}
