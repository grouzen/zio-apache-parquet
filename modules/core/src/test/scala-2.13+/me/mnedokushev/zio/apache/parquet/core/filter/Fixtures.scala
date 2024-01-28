package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.io.api.Binary
import zio.Chunk
import zio.schema._

import java.time._
import java.util.UUID

object Fixtures {

  case class MyRecord(a: String, b: Int, child: MyRecord.Child, enm: MyRecord.Enum, opt: Option[Int])

  object MyRecord {
    implicit val schema: Schema.CaseClass5.WithFields[
      "a",
      "b",
      "child",
      "enm",
      "opt",
      String,
      Int,
      MyRecord.Child,
      MyRecord.Enum,
      Option[Int],
      MyRecord
    ] =
      DeriveSchema.gen[MyRecord]
    implicit val typeTag: TypeTag[MyRecord] =
      Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    case class Child(c: Int, d: Option[Long])
    object Child {
      implicit val schema: Schema.CaseClass2.WithFields["c", "d", Int, Option[Long], MyRecord.Child] =
        DeriveSchema.gen[Child]
      implicit val typeTag: TypeTag[Child]                                                           =
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
    implicit val schema: zio.schema.Schema.CaseClass2.WithFields["a", "b", Int, String, MyRecordSummoned] =
      DeriveSchema.gen[MyRecordSummoned]

    implicit val intTypeTag: TypeTag.EqNotEq[Int]   =
      TypeTag.eqnoteq[Int, Binary, BinaryColumn](
        FilterApi.binaryColumn,
        v => Value.string(v.toString).value
      )
    implicit val typeTag: TypeTag[MyRecordSummoned] = Derive.derive[TypeTag, MyRecordSummoned](TypeTagDeriver.summoned)
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
    implicit val schema: zio.schema.Schema.CaseClass21.WithFields[
      "string",
      "boolean",
      "byte",
      "short",
      "int",
      "long",
      "float",
      "double",
      "binary",
      "char",
      "uuid",
      "bigDecimal",
      "bigInteger",
      "dayOfWeek",
      "month",
      "monthDay",
      "period",
      "year",
      "yearMonth",
      "zoneId",
      "zoneOffset",
      String,
      Boolean,
      Byte,
      Short,
      Int,
      Long,
      Float,
      Double,
      zio.Chunk[Byte],
      Char,
      java.util.UUID,
      java.math.BigDecimal,
      java.math.BigInteger,
      java.time.DayOfWeek,
      java.time.Month,
      java.time.MonthDay,
      java.time.Period,
      java.time.Year,
      java.time.YearMonth,
      java.time.ZoneId,
      java.time.ZoneOffset,
      me.mnedokushev.zio.apache.parquet.core.filter.Fixtures.MyRecordAllTypes1
    ] =
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
    implicit val schema: zio.schema.Schema.CaseClass8.WithFields[
      "duration",
      "instant",
      "localDate",
      "localTime",
      "localDateTime",
      "offsetTime",
      "offsetDateTime",
      "zonedDateTime",
      java.time.Duration,
      java.time.Instant,
      java.time.LocalDate,
      java.time.LocalTime,
      java.time.LocalDateTime,
      java.time.OffsetTime,
      java.time.OffsetDateTime,
      java.time.ZonedDateTime,
      me.mnedokushev.zio.apache.parquet.core.filter.Fixtures.MyRecordAllTypes2
    ] =
      DeriveSchema.gen[MyRecordAllTypes2]
    implicit val typeTag: TypeTag[MyRecordAllTypes2] =
      Derive.derive[TypeTag, MyRecordAllTypes2](TypeTagDeriver.default)
  }

}
