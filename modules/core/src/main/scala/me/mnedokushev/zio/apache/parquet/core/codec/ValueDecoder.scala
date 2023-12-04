package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Value

trait ValueDecoder[+A] {

  def decode(value: Value): A

}
