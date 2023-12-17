package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._

trait ValueEncoder[-A] { self =>

  def encode(value: A): Value

  def encodeZIO(value: A): Task[Value] =
    ZIO.attemptBlocking(encode(value))

  def contramap[B](f: B => A): ValueEncoder[B] =
    new ValueEncoder[B] {
      override def encode(value: B): Value =
        self.encode(f(value))
    }

}
