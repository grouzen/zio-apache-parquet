package me.mnedokushev.zio.apache.parquet.core.filter

import zio.schema._

case class MyRecord(a: String, b: Int, child: MyRecord.Child)

object MyRecord {
  implicit val schema                     = DeriveSchema.gen[MyRecord]
  implicit val typeTag: TypeTag[MyRecord] = Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

  case class Child(c: Int, d: Option[Long])
  object Child {
    implicit val schema                  = DeriveSchema.gen[Child]
    implicit val typeTag: TypeTag[Child] = Derive.derive[TypeTag, Child](TypeTagDeriver.default)

  }

}
