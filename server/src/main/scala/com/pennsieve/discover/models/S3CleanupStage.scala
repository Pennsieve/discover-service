// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.pennsieve.discover.models

object S3CleanupStage {
  def Initial = "INITIAL"
  def Failure = "FAILURE"
  def Unpublish = "UNPUBLISH"
  def Tidy = "TIDY"
}
