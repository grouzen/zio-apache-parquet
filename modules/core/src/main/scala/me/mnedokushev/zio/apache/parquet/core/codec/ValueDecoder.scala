package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._

trait ValueDecoder[+A] {

  def decode(value: Value): A

  def decodeZIO(value: Value): Task[A] =
    ZIO.attempt(decode(value))

}
