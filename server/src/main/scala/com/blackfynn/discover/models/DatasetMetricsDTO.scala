// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.discover.server.definitions
import io.scalaland.chimney.dsl._

object DatasetMetricsDTO {
  def apply(
    datasets: IndexedSeq[DatasetMetrics]
  ): definitions.DatasetMetricsDTO = {
    definitions.DatasetMetricsDTO(
      datasets = datasets.map(_.into[definitions.DatasetMetrics].transform)
    )
  }
}
