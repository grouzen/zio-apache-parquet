package me.mnedokushev.zio.apache.parquet.core.codec

import org.apache.parquet.schema.Type
import zio._
import zio.schema._

trait SchemaEncoder[A] {

  def encode(schema: Schema[A], name: String, optional: Boolean): Type

  def encodeZIO(schema: Schema[A], name: String, optional: Boolean): Task[Type] =
    ZIO.attempt(encode(schema, name, optional))

}
