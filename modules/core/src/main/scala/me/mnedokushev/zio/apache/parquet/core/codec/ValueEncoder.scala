package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._

import java.io.IOException

trait ValueEncoder[-A] {

  def encode(value: A): Value

  def encodeZIO(value: A): IO[IOException, Value] =
    ZIO.attemptBlockingIO(encode(value))

}
