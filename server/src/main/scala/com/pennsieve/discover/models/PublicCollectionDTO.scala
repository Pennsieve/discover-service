// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.server.definitions
import com.pennsieve.discover.Config
import cats.implicits._
import io.scalaland.chimney.dsl._

object PublicCollectionDTO {

  def apply(collection: PublicCollection): definitions.PublicCollectionDto = {

    collection
      .into[definitions.PublicCollectionDto]
      .transform
  }

}
