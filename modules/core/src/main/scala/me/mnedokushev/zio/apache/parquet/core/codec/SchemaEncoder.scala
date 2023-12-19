package me.mnedokushev.zio.apache.parquet.core.codec

import org.apache.parquet.schema.Type
import zio._
import zio.schema._

trait SchemaEncoder[A] { self =>

  def encode(schema: Schema[A], name: String, optional: Boolean): Type

  def encodeZIO(schema: Schema[A], name: String, optional: Boolean): Task[Type] =
    ZIO.attempt(encode(schema, name, optional))

  def contramap[B](f: Schema[B] => Schema[A]): SchemaEncoder[B] =
    new SchemaEncoder[B] {
      override def encode(schema: Schema[B], name: String, optional: Boolean): Type =
        self.encode(f(schema), name, optional)
    }

}
