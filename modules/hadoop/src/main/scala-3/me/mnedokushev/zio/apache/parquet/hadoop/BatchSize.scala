package me.mnedokushev.zio.apache.parquet.hadoop

import zio.prelude.{ Assertion, Newtype }

object BatchSize extends Newtype[Int] {
  override inline def assertion = Assertion.greaterThan(0)
}
