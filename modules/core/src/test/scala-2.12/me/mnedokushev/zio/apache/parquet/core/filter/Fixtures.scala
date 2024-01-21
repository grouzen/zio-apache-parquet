package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.io.api.Binary
import zio._
import zio.schema._

import java.time._
import java.util.UUID

object Fixtures {

  case class MyRecord(a: String, b: Int, child: MyRecord.Child, enm: MyRecord.Enum)

  object MyRecord {
    implicit val schema                     =
      DeriveSchema.gen[MyRecord]
    implicit val typeTag: TypeTag[MyRecord] =
      Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    case class Child(c: Int, d: Option[Long])
    object Child {
      implicit val schema                  =
        DeriveSchema.gen[Child]
      implicit val typeTag: TypeTag[Child] =
        Derive.derive[TypeTag, Child](TypeTagDeriver.default)
    }

    sealed trait Enum
    object Enum {
      case object Started    extends Enum
      case object InProgress extends Enum
      case object Done       extends Enum

      implicit val schema: Schema[Enum]   =
        DeriveSchema.gen[Enum]
      implicit val typeTag: TypeTag[Enum] =
        Derive.derive[TypeTag, Enum](TypeTagDeriver.default)
    }

  }

  case class MyRecordSummoned(a: Int, b: String)

  object MyRecordSummoned  {
    implicit val schema =
      DeriveSchema.gen[MyRecordSummoned]

    implicit val intTypeTag: TypeTag.EqNotEq[Int]   =
      TypeTag.eqnoteq[Int, Binary, BinaryColumn](
        FilterApi.binaryColumn,
        v => Value.string(v.toString).value
      )
    implicit val typeTag: TypeTag[MyRecordSummoned] =
      Derive.derive[TypeTag, MyRecordSummoned](TypeTagDeriver.summoned)
  }

  case class MyRecordAllTypes1(
    string: String,
    boolean: Boolean,
    byte: Byte,
    short: Short,
    int: Int,
    long: Long,
    float: Float,
    double: Double,
    binary: Chunk[Byte],
    char: Char,
    uuid: UUID,
    bigDecimal: java.math.BigDecimal,
    bigInteger: java.math.BigInteger,
    dayOfWeek: DayOfWeek,
    month: Month,
    monthDay: MonthDay,
    period: Period,
    year: Year,
    yearMonth: YearMonth,
    zoneId: ZoneId,
    zoneOffset: ZoneOffset
  )
  object MyRecordAllTypes1 {
    implicit val schema                              =
      DeriveSchema.gen[MyRecordAllTypes1]
    implicit val typeTag: TypeTag[MyRecordAllTypes1] =
      Derive.derive[TypeTag, MyRecordAllTypes1](TypeTagDeriver.default)
  }
  case class MyRecordAllTypes2(
    duration: Duration,
    instant: Instant,
    localDate: LocalDate,
    localTime: LocalTime,
    localDateTime: LocalDateTime,
    offsetTime: OffsetTime,
    offsetDateTime: OffsetDateTime,
    zonedDateTime: ZonedDateTime
  )
  object MyRecordAllTypes2 {
    implicit val schema                              =
      DeriveSchema.gen[MyRecordAllTypes2]
    implicit val typeTag: TypeTag[MyRecordAllTypes2] =
      Derive.derive[TypeTag, MyRecordAllTypes2](TypeTagDeriver.default)
  }

}
