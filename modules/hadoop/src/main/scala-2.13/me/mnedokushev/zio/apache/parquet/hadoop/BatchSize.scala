package me.mnedokushev.zio.apache.parquet.hadoop

import zio.prelude.{ Assertion, Newtype }

import scala.annotation.nowarn

object BatchSize extends Newtype[Int] {
  @nowarn("cat=scala3-migration")
  override def assertion = assert(Assertion.greaterThan(0))
}
