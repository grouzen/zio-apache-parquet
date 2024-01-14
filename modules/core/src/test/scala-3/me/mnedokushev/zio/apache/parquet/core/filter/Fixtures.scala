package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.io.api.Binary
import zio.schema._
object Fixtures {

  case class MyRecord(a: String, b: Int, child: MyRecord.Child)

  object MyRecord {
    implicit val schema: Schema.CaseClass3.WithFields["a", "b", "child", String, Int, MyRecord.Child, MyRecord] =
      DeriveSchema.gen[MyRecord]
    implicit val typeTag: TypeTag[MyRecord]                                                                     =
      Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    case class Child(c: Int, d: Option[Long])
    object Child {
      implicit val schema: Schema.CaseClass2.WithFields["c", "d", Int, Option[Long], MyRecord.Child] =
        DeriveSchema.gen[Child]
      implicit val typeTag: TypeTag[Child]                                                           =
        Derive.derive[TypeTag, Child](TypeTagDeriver.default)

    }
  }

  case class MyRecordSummoned(a: Int, b: String)

  object MyRecordSummoned {
    implicit val schema: zio.schema.Schema.CaseClass2.WithFields["a", "b", Int, String, MyRecordSummoned] =
      DeriveSchema.gen[MyRecordSummoned]

    implicit val intTypeTag: TypeTag.EqNotEq[Int]   =
      TypeTag.eqnoteq[Int, Binary, BinaryColumn](
        FilterApi.binaryColumn,
        v => Value.string(v.toString).value
      )
    implicit val typeTag: TypeTag[MyRecordSummoned] = Derive.derive[TypeTag, MyRecordSummoned](TypeTagDeriver.summoned)
  }

}
