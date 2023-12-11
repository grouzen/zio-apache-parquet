package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._

trait ValueEncoder[-A] {

  def encode(value: A): Value

  def encodeZIO(value: A): Task[Value] =
    ZIO.attemptBlocking(encode(value))

}
