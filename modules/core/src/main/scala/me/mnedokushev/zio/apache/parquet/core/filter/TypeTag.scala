package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.{
  BinaryColumn,
  BooleanColumn,
  Column,
  IntColumn,
  LongColumn,
  SupportsEqNotEq,
  SupportsLtGt
}
import org.apache.parquet.io.api.Binary

sealed trait TypeTag[+A]

object TypeTag {

  trait Dummy[A] extends TypeTag[A]

  final case class Record[A](columns: Map[String, TypeTag[_]]) extends TypeTag[A]

  sealed trait EqNotEq[A] extends TypeTag[A] { self =>
    type T <: Comparable[T]
    type C <: Column[T] with SupportsEqNotEq

    def cast[A0]: EqNotEq[A0] = self.asInstanceOf[EqNotEq[A0]]

    def column(path: String): C
    def value(v: A): T
  }

  sealed trait LtGt[A] extends TypeTag[A] { self =>
    type T <: Comparable[T]
    type C <: Column[T] with SupportsLtGt

    def cast[A0]: LtGt[A0] = self.asInstanceOf[LtGt[A0]]

    def column(path: String): C
    def value(v: A): T
  }

  def dummy[A]: TypeTag.Dummy[A] =
    new Dummy[A] {}

  implicit case object TString extends TypeTag.EqNotEq[String] {
    override type T = Binary
    override type C = BinaryColumn

    override def column(path: String): C =
      FilterApi.binaryColumn(path)

    override def value(v: String): T =
      Value.string(v).value

  }

  implicit case object TBoolean extends TypeTag.EqNotEq[Boolean] {
    override type T = java.lang.Boolean
    override type C = BooleanColumn

    override def column(path: String): C =
      FilterApi.booleanColumn(path)

    override def value(v: Boolean): T =
      Value.boolean(v).value

  }

  implicit case object TByte extends TypeTag.LtGt[Byte] {
    override type T = java.lang.Integer
    override type C = IntColumn

    override def column(path: String): C =
      FilterApi.intColumn(path)

    override def value(v: Byte): T =
      Value.byte(v).value

  }

  implicit case object TShort extends TypeTag.LtGt[Short] {
    override type T = java.lang.Integer
    override type C = IntColumn

    override def column(path: String): C =
      FilterApi.intColumn(path)

    override def value(v: Short): T =
      Value.short(v).value

  }
  implicit case object TInt extends TypeTag.LtGt[Int] {
    override type T = java.lang.Integer
    override type C = IntColumn

    override def column(path: String): C =
      FilterApi.intColumn(path)

    override def value(v: Int): T =
      Value.int(v).value

  }

  implicit case object TLong extends TypeTag.LtGt[Long] {
    override type T = java.lang.Long
    override type C = LongColumn

    override def column(path: String): C =
      FilterApi.longColumn(path)

    override def value(v: Long): T =
      Value.long(v).value

  }

}
