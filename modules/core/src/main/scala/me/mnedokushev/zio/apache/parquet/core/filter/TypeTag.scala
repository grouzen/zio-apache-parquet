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

import scala.jdk.CollectionConverters._

trait TypeTag[+A]

object TypeTag {

  trait Dummy[+A] extends TypeTag[A]

  def dummy[A]: TypeTag.Dummy[A] =
    new Dummy[A] {}

  final case class Optional[+A: TypeTag]() extends TypeTag[Option[A]] {
    val typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

  implicit def optional[A: TypeTag]: TypeTag[Option[A]] =
    Optional[A]()

  final case class Record[+A](columns: Map[String, TypeTag[_]]) extends TypeTag[A]

  trait EqNotEq[A] extends TypeTag[A] { self =>
    type T <: Comparable[T]
    type C <: Column[T] with SupportsEqNotEq

    def cast[A0]: EqNotEq[A0] = self.asInstanceOf[EqNotEq[A0]]

    def column(path: String): C
    def value(v: A): T
    def values(vs: Set[A]): java.util.Set[T] =
      vs.map(value).asJava
  }

  trait LtGt[A] extends TypeTag[A] { self =>
    type T <: Comparable[T]
    type C <: Column[T] with SupportsLtGt

    def cast[A0]: LtGt[A0] = self.asInstanceOf[LtGt[A0]]

    def column(path: String): C
    def value(v: A): T
    def values(vs: Set[A]): java.util.Set[T] =
      vs.map(value).asJava
  }

  def eqnoteq[A, T0 <: Comparable[T0], C0 <: Column[T0] with SupportsEqNotEq](
    column0: String => C0,
    value0: A => T0
  ): TypeTag.EqNotEq[A] =
    new TypeTag.EqNotEq[A] {

      override type T = T0

      override type C = C0

      override def column(path: String): C =
        column0(path)

      override def value(v: A): T =
        value0(v)

    }

  def ltgt[A, T0 <: Comparable[T0], C0 <: Column[T0] with SupportsLtGt](
    column0: String => C0,
    value0: A => T0
  ): TypeTag.LtGt[A] =
    new TypeTag.LtGt[A] {

      override type T = T0

      override type C = C0

      override def column(path: String): C =
        column0(path)

      override def value(v: A): T =
        value0(v)

    }

  implicit val string: TypeTag.EqNotEq[String]   =
    eqnoteq[String, Binary, BinaryColumn](
      FilterApi.binaryColumn,
      Value.string(_).value
    )
  implicit val boolean: TypeTag.EqNotEq[Boolean] =
    eqnoteq[Boolean, java.lang.Boolean, BooleanColumn](
      FilterApi.booleanColumn,
      Value.boolean(_).value
    )
  implicit val byte: TypeTag.LtGt[Byte]          =
    ltgt[Byte, java.lang.Integer, IntColumn](
      FilterApi.intColumn,
      Value.byte(_).value
    )
  implicit val short: TypeTag.LtGt[Short]        =
    ltgt[Short, java.lang.Integer, IntColumn](
      FilterApi.intColumn,
      Value.short(_).value
    )
  implicit val int: TypeTag.LtGt[Int]            =
    ltgt[Int, java.lang.Integer, IntColumn](
      FilterApi.intColumn,
      Value.int(_).value
    )
  implicit val long: TypeTag.LtGt[Long]          =
    ltgt[Long, java.lang.Long, LongColumn](
      FilterApi.longColumn,
      Value.long(_).value
    )

}
