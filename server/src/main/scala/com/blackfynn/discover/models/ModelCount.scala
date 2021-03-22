// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import com.blackfynn.discover.server.definitions

object ModelCount {

  def apply(modelName: String, count: Long): definitions.ModelCount =
    definitions.ModelCount(modelName, count)

}
