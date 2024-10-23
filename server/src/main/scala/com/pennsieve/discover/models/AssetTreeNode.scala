// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.{ utils, Config }
import com.pennsieve.discover.server.definitions
import io.scalaland.chimney.dsl._

case class AssetTreeNode(`type`: String, file: String, name: String, size: Long)

object AssetTreeNodeDTO {

  def apply(
    node: AssetTreeNode
  )(implicit
    config: Config
  ): definitions.AssetTreeNodeDto =
    node
      .into[definitions.AssetTreeNodeDto]
      .withFieldComputed(_.name, _ => node.name)
      .withFieldComputed(_.path, _ => node.file)
      .withFieldComputed(_.`type`, _ => node.`type`)
      .withFieldComputed(_.size, _ => node.size)
      .transform
}
