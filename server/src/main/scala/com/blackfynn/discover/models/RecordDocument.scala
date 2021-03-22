// Copyright (c) 2019 Pennsieve, Inc. All Rights Reserved.

package com.blackfynn.discover.models

import io.circe.{ Decoder, Encoder }
import io.circe.generic.semiauto.{ deriveDecoder, deriveEncoder }

/**
  * Case class used to index records in ElasticSearch.
  */
final case class RecordDocument(record: Record)

object RecordDocument {
  implicit val encoder: Encoder[RecordDocument] = deriveEncoder[RecordDocument]
  implicit val decoder: Decoder[RecordDocument] = deriveDecoder[RecordDocument]
}
