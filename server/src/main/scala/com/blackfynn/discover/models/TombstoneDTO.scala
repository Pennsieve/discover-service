// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import cats.implicits._
import com.pennsieve.discover.server.definitions
import io.scalaland.chimney.dsl._

object TombstoneDTO {

  def apply(
    dataset: PublicDataset,
    version: PublicDatasetVersion
  ): definitions.TombstoneDTO = {
    version
      .into[definitions.TombstoneDTO]
      .withFieldComputed(_.id, _ => dataset.id)
      .withFieldComputed(_.name, _ => dataset.name)
      .withFieldComputed(_.tags, _ => dataset.tags.toIndexedSeq)
      .transform
  }
}
