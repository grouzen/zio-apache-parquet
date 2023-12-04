package me.mnedokushev.zio.apache.parquet.core.codec

import org.apache.parquet.schema.Type
import zio.schema.Schema

trait SchemaEncoder[A] {

  def encode(schema: Schema[A], name: String, optional: Boolean): Type

}
