package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema._

case class MyRecord(a: String, b: Int)

object MyRecord {
  implicit val recordSchema = DeriveSchema.gen[MyRecord]

  implicit val typeTag: TypeTag[MyRecord] = Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)
}
