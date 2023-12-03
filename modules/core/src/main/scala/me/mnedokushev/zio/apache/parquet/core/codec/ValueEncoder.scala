package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value

trait ValueEncoder[A] {

  def encode(value: A): Value

}
