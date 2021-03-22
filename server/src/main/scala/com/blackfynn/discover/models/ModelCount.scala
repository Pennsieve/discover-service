// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

import com.pennsieve.discover.server.definitions

object ModelCount {

  def apply(modelName: String, count: Long): definitions.ModelCount =
    definitions.ModelCount(modelName, count)

}
