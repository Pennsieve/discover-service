// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import cats.implicits._
import com.pennsieve.discover.{ utils, Config }
import com.pennsieve.discover.server.definitions
import io.scalaland.chimney.dsl._

object FileTreeNodeDTO {

  def apply(
    node: FileTreeNode
  )(implicit
    config: Config
  ): definitions.FileTreeNodeDto =
    node match {
      case f: FileTreeNode.File =>
        f.into[definitions.File]
          .withFieldComputed(_.uri, _ => s"s3://${f.s3Bucket}/${f.s3Key}")
          .withFieldComputed(
            _.packageType,
            _ => utils.getPackageType(f.fileType)
          )
          .withFieldComputed(_.icon, _ => utils.getIcon(f.fileType))
          .transform
      case d: FileTreeNode.Directory => d.into[definitions.Directory].transform
    }
}
