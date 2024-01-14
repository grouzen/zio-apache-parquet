package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.io.api.Binary
import zio.schema._

object Fixtures {

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

  case class MyRecordSummoned(a: Int, b: String)

  object MyRecordSummoned {
    implicit val schema = DeriveSchema.gen[MyRecordSummoned]

    implicit val intTypeTag: TypeTag.EqNotEq[Int]   =
      TypeTag.eqnoteq[Int, Binary, BinaryColumn](
        FilterApi.binaryColumn,
        v => Value.string(v.toString).value
      )
    implicit val typeTag: TypeTag[MyRecordSummoned] = Derive.derive[TypeTag, MyRecordSummoned](TypeTagDeriver.summoned)
  }

}
