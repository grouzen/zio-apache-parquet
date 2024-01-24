package me.mnedokushev.zio.apache.parquet.core.filter

import zio.{ Chunk, Duration }

import java.time.{
  DayOfWeek,
  Instant,
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
import scala.annotation.implicitNotFound

sealed trait OperatorSupport[A]

object OperatorSupport {

  final class Optional[A: TypeTag: OperatorSupport] extends OperatorSupport[Option[A]] {
    val typeTag: TypeTag[A]                 = implicitly[TypeTag[A]]
    val operatorSupport: OperatorSupport[A] = implicitly[OperatorSupport[A]]
  }

  @implicitNotFound("You can't use this operator for the type ${A}")
  abstract class LtGt[A: TypeTag] extends OperatorSupport[A] {
    val typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

  object LtGt {

    implicit def optional[A: TypeTag: LtGt]: LtGt[Option[A]] =
      new LtGt[Option[A]] {}

    implicit case object byte           extends LtGt[Byte]
    implicit case object short          extends LtGt[Short]
    implicit case object int            extends LtGt[Int]
    implicit case object long           extends LtGt[Long]
    implicit case object float          extends LtGt[Float]
    implicit case object double         extends LtGt[Double]
    implicit case object bigDecimal     extends LtGt[java.math.BigDecimal]
    implicit case object bigInteger     extends LtGt[java.math.BigInteger]
    implicit case object dayOfWeek      extends LtGt[DayOfWeek]
    implicit case object month          extends LtGt[Month]
    implicit case object monthDay       extends LtGt[MonthDay]
    implicit case object period         extends LtGt[Period]
    implicit case object year           extends LtGt[Year]
    implicit case object yearMonth      extends LtGt[YearMonth]
    implicit case object duration       extends LtGt[Duration]
    implicit case object instant        extends LtGt[Instant]
    implicit case object localDate      extends LtGt[LocalDate]
    implicit case object localTime      extends LtGt[LocalTime]
    implicit case object localDateTime  extends LtGt[LocalDateTime]
    implicit case object offsetTime     extends LtGt[OffsetTime]
    implicit case object offsetDateTime extends LtGt[OffsetDateTime]
    implicit case object zonedDateTime  extends LtGt[ZonedDateTime]

  }

  @implicitNotFound("You can't use this operator for the type ${A}")
  abstract class EqNotEq[A: TypeTag] extends OperatorSupport[A] {
    val typeTag: TypeTag[A] = implicitly[TypeTag[A]]
  }

  object EqNotEq {

    implicit def enum0[A: TypeTag]: EqNotEq[A] = new EqNotEq[A] {}

    implicit def optional[A: TypeTag: EqNotEq]: EqNotEq[Option[A]] =
      new EqNotEq[Option[A]] {}

    implicit case object string         extends EqNotEq[String]
    implicit case object boolean        extends EqNotEq[Boolean]
    implicit case object byte           extends EqNotEq[Byte]
    implicit case object short          extends EqNotEq[Short]
    implicit case object int            extends EqNotEq[Int]
    implicit case object long           extends EqNotEq[Long]
    implicit case object float          extends EqNotEq[Float]
    implicit case object double         extends EqNotEq[Double]
    implicit case object binary         extends EqNotEq[Chunk[Byte]]
    implicit case object char           extends EqNotEq[Char]
    implicit case object uuid           extends EqNotEq[UUID]
    implicit case object bigDecimal     extends EqNotEq[java.math.BigDecimal]
    implicit case object bigInteger     extends EqNotEq[java.math.BigInteger]
    implicit case object dayOfWeek      extends EqNotEq[DayOfWeek]
    implicit case object month          extends EqNotEq[Month]
    implicit case object monthDay       extends EqNotEq[MonthDay]
    implicit case object period         extends EqNotEq[Period]
    implicit case object year           extends EqNotEq[Year]
    implicit case object yearMonth      extends EqNotEq[YearMonth]
    implicit case object zoneId         extends EqNotEq[ZoneId]
    implicit case object zoneOffset     extends EqNotEq[ZoneOffset]
    implicit case object duration       extends EqNotEq[Duration]
    implicit case object instant        extends EqNotEq[Instant]
    implicit case object localDate      extends EqNotEq[LocalDate]
    implicit case object localTime      extends EqNotEq[LocalTime]
    implicit case object localDateTime  extends EqNotEq[LocalDateTime]
    implicit case object offsetTime     extends EqNotEq[OffsetTime]
    implicit case object offsetDateTime extends EqNotEq[OffsetDateTime]
    implicit case object zonedDateTime  extends EqNotEq[ZonedDateTime]

  }

}
