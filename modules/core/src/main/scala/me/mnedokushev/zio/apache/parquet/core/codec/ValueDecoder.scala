package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value
import zio._

trait ValueDecoder[+A] { self =>

  def decode(value: Value): A

  def decodeZIO(value: Value): Task[A] =
    ZIO.attempt(decode(value))

  def map[B](f: A => B): ValueDecoder[B] =
    new ValueDecoder[B] {
      override def decode(value: Value): B =
        f(self.decode(value))
    }

}
