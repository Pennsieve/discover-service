// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.discover.server.definitions
import com.blackfynn.discover.Config
import cats.implicits._
import io.scalaland.chimney.dsl._

object PublicCollectionDTO {

  def apply(collection: PublicCollection): definitions.PublicCollectionDTO = {

    collection
      .into[definitions.PublicCollectionDTO]
      .transform
  }

}
