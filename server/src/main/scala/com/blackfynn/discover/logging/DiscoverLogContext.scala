// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.logging

import com.blackfynn.service.utilities.LogContext

final case class DiscoverLogContext(
  organizationId: Option[Int] = None,
  datasetId: Option[Int] = None,
  userId: Option[Int] = None,
  publicDatasetId: Option[Int] = None,
  publicDatasetVersion: Option[Int] = None
) extends LogContext {
  override val values: Map[String, String] = inferValues(this)
}
