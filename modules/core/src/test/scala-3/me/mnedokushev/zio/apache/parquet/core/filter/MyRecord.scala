package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema._

case class MyRecord(a: String, b: Int)

object MyRecord {
  type A = "a"
  type B = "b"
  implicit val recordSchema: Schema.CaseClass2.WithFields[A, B, String, Int, MyRecord] =
    DeriveSchema.gen[MyRecord]

  implicit val typeTag: TypeTag[MyRecord] = Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)
}
