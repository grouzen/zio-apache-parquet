package me.mnedokushev.zio.apache.parquet.core.filter

import _root_.java.time.Instant
import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.filter.TypeTag.{ Dummy, EqNotEq, LtGt, Optional, Record }
import org.apache.parquet.filter2.predicate.{FilterApi, Operators}
import org.apache.parquet.io.api.Binary
import zio.{ Chunk, Duration }

import java.time.{
  DayOfWeek,
  LocalDate,
  LocalDateTime,
  LocalTime,
  Month,
  MonthDay,
  OffsetDateTime,
  OffsetTime,
  Period,
  Year,
  YearMonth,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.UUID
import scala.jdk.CollectionConverters._

sealed trait TypeTag[+A] { self =>

  override def toString: String =
    self match {
      case _: Dummy[_]    => "Dummy[A]"
      case _: Optional[_] => "Optional[A]"
      case _: Record[_]   => "Record[A]"
      case _: EqNotEq[_]  => "EqNotEq[A]"
      case _: LtGt[_]     => "LtGt[A]"
    }

}

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
    type C <: Operators.Column[T] with Operators.SupportsEqNotEq

    def cast[A0]: EqNotEq[A0] = self.asInstanceOf[EqNotEq[A0]]

    def column(path: String): C
    def value(v: A): T
    def values(vs: Set[A]): java.util.Set[T] =
      vs.map(value).asJava
  }

  trait LtGt[A] extends TypeTag[A] { self =>
    type T <: Comparable[T]
    type C <: Operators.Column[T] with Operators.SupportsLtGt

    def cast[A0]: LtGt[A0] = self.asInstanceOf[LtGt[A0]]

    def column(path: String): C
    def value(v: A): T
    def values(vs: Set[A]): java.util.Set[T] =
      vs.map(value).asJava
  }

  def eqnoteq[A, T0 <: Comparable[T0], C0 <: Operators.Column[T0] with Operators.SupportsEqNotEq](
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

  def ltgt[A, T0 <: Comparable[T0], C0 <: Operators.Column[T0] with Operators.SupportsLtGt](
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

  def enum0[A](casesMap: Map[A, String]): TypeTag.EqNotEq[A] =
    eqnoteq[A, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      v => Value.string(casesMap.getOrElse(v, throw FilterError(s"Failed to encode enum for value $v"))).value
    )

  implicit val string: TypeTag.EqNotEq[String]                =
    eqnoteq[String, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.string(_).value
    )
  implicit val boolean: TypeTag.EqNotEq[Boolean]              =
    eqnoteq[Boolean, java.lang.Boolean, Operators.BooleanColumn](
      FilterApi.booleanColumn,
      Value.boolean(_).value
    )
  implicit val byte: TypeTag.LtGt[Byte]                       =
    ltgt[Byte, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.byte(_).value
    )
  implicit val short: TypeTag.LtGt[Short]                     =
    ltgt[Short, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.short(_).value
    )
  implicit val int: TypeTag.LtGt[Int]                         =
    ltgt[Int, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.int(_).value
    )
  implicit val long: TypeTag.LtGt[Long]                       =
    ltgt[Long, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.long(_).value
    )
  implicit val float: TypeTag.LtGt[Float]                     =
    ltgt[Float, java.lang.Float, Operators.FloatColumn](
      FilterApi.floatColumn,
      Value.float(_).value
    )
  implicit val double: TypeTag.LtGt[Double]                   =
    ltgt[Double, java.lang.Double, Operators.DoubleColumn](
      FilterApi.doubleColumn,
      Value.double(_).value
    )
  implicit val binary: TypeTag.EqNotEq[Chunk[Byte]]           =
    eqnoteq[Chunk[Byte], Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.binary(_).value
    )
  implicit val char: TypeTag.EqNotEq[Char]                    =
    eqnoteq[Char, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.char(_).value
    )
  implicit val uuid: TypeTag.EqNotEq[UUID]                    =
    eqnoteq[UUID, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.uuid(_).value
    )
  implicit val bigDecimal: TypeTag.LtGt[java.math.BigDecimal] =
    ltgt[java.math.BigDecimal, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.bigDecimal(_).value
    )
  implicit val bigInteger: TypeTag.LtGt[java.math.BigInteger] =
    ltgt[java.math.BigInteger, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.bigInteger(_).value
    )
  implicit val dayOfWeek: TypeTag.LtGt[DayOfWeek]             =
    ltgt[DayOfWeek, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.dayOfWeek(_).value
    )
  implicit val month: TypeTag.LtGt[Month]                     =
    ltgt[Month, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.month(_).value
    )
  implicit val monthDay: TypeTag.LtGt[MonthDay]               =
    ltgt[MonthDay, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.monthDay(_).value
    )
  implicit val period: TypeTag.LtGt[Period]                   =
    ltgt[Period, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.period(_).value
    )
  implicit val year: TypeTag.LtGt[Year]                       =
    ltgt[Year, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.year(_).value
    )
  implicit val yearMonth: TypeTag.LtGt[YearMonth]             =
    ltgt[YearMonth, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.yearMonth(_).value
    )
  // NOTE: it is not implicit to make scalac happy since ZoneOffset is a subtype of ZoneId
  val zoneId: TypeTag.EqNotEq[ZoneId]                         =
    eqnoteq[ZoneId, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.zoneId(_).value
    )
  implicit val zoneOffset: TypeTag.EqNotEq[ZoneOffset]        =
    eqnoteq[ZoneOffset, Binary, Operators.BinaryColumn](
      FilterApi.binaryColumn,
      Value.zoneOffset(_).value
    )
  implicit val duration: TypeTag.LtGt[Duration]               =
    ltgt[Duration, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.duration(_).value
    )
  implicit val instant: TypeTag.LtGt[Instant]                 =
    ltgt[Instant, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.instant(_).value
    )
  implicit val localDate: TypeTag.LtGt[LocalDate]             =
    ltgt[LocalDate, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.localDate(_).value
    )
  implicit val localTime: TypeTag.LtGt[LocalTime]             =
    ltgt[LocalTime, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.localTime(_).value
    )
  implicit val localDateTime: TypeTag.LtGt[LocalDateTime]     =
    ltgt[LocalDateTime, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.localDateTime(_).value
    )
  implicit val offsetTime: TypeTag.LtGt[OffsetTime]           =
    ltgt[OffsetTime, java.lang.Integer, Operators.IntColumn](
      FilterApi.intColumn,
      Value.offsetTime(_).value
    )
  implicit val offsetDateTime: TypeTag.LtGt[OffsetDateTime]   =
    ltgt[OffsetDateTime, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.offsetDateTime(_).value
    )
  implicit val zonedDateTime: TypeTag.LtGt[ZonedDateTime]     =
    ltgt[ZonedDateTime, java.lang.Long, Operators.LongColumn](
      FilterApi.longColumn,
      Value.zonedDateTime(_).value
    )

}
