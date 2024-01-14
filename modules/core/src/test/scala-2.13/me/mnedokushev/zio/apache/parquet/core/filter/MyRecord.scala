package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema._

case class MyRecord(a: String, b: Int, child: MyRecord.Child)

object MyRecord {
  type A      = "a"
  type B      = "b"
  type Child0 = "child"
  implicit val schema: Schema.CaseClass3.WithFields[A, B, Child0, String, Int, MyRecord.Child, MyRecord] =
    DeriveSchema.gen[MyRecord]
  implicit val typeTag: TypeTag[MyRecord]                                                                =
    Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

  case class Child(c: Int, d: Option[Long])
  object Child {
    type C = "c"
    type D = "d"
    implicit val schema: Schema.CaseClass2.WithFields[C, D, Int, Option[Long], MyRecord.Child] =
      DeriveSchema.gen[Child]
    implicit val typeTag: TypeTag[Child]                                                       =
      Derive.derive[TypeTag, Child](TypeTagDeriver.default)

  }
}
