// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.logging

import com.pennsieve.service.utilities.LogContext

final case class DiscoverLogContext(
  organizationId: Option[Int] = None,
  datasetId: Option[Int] = None,
  userId: Option[Int] = None,
  publicDatasetId: Option[Int] = None,
  publicDatasetVersion: Option[Int] = None
) extends LogContext {
  override val values: Map[String, String] = inferValues(this)
}
