package me.mnedokushev.zio.apache.parquet.core

import me.mnedokushev.zio.apache.parquet.core.codec.{
  SchemaEncoder,
  SchemaEncoderDeriver,
  ValueDecoder,
  ValueDecoderDeriver,
  ValueEncoder,
  ValueEncoderDeriver
}
import me.mnedokushev.zio.apache.parquet.core.filter.{ TypeTag, TypeTagDeriver }
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn
import org.apache.parquet.io.api.Binary
import zio.Chunk
import zio.schema._

import java.time._
import java.util.{ Currency, UUID }

object Fixtures {

  // unable to generate code for case classes with more than 120 int fields due to following error:
  // tested with jdk 11.0.23 64bit
  // Error while emitting me/mnedokushev/zio/apache/parquet/core/codec/SchemaEncoderDeriverSpec$MaxArityRecord$
  // Method too large: me/mnedokushev/zio/apache/parquet/core/codec/SchemaEncoderDeriverSpec$MaxArityRecord$.derivedSchema0$lzyINIT4$1$$anonfun$364 (Lscala/collection/immutable/ListMap;)Lscala/util/Either;
  case class Arity23(
    a: Int,
    b: Option[String],
    c: Int,
    d: Int,
    e: Int,
    f: Int,
    g: Int,
    h: Int,
    i: Int,
    j: Int,
    k: Int,
    l: Int,
    m: Int,
    n: Int,
    o: Int,
    p: Int,
    q: Int,
    r: Int,
    s: Int,
    t: Int,
    u: Int,
    v: Int,
    w: Int
  )
  object Arity23 {
    implicit lazy val schema: Schema[Arity23] =
      DeriveSchema.gen[Arity23]
  }

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
    ]                                       =
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

  case class MyRecordIO(a: Int, b: String, c: Option[Long], d: List[Int], e: Map[String, Int])
  object MyRecordIO {
    implicit val schema: zio.schema.Schema.CaseClass5.WithFields[
      "a",
      "b",
      "c",
      "d",
      "e",
      Int,
      String,
      Option[Long],
      List[Int],
      Map[String, Int],
      MyRecordIO
    ]                                                     =
      DeriveSchema.gen[MyRecordIO]
    implicit val schemaEncoder: SchemaEncoder[MyRecordIO] =
      Derive.derive[SchemaEncoder, MyRecordIO](SchemaEncoderDeriver.summoned)
    implicit val valueEncoder: ValueEncoder[MyRecordIO]   =
      Derive.derive[ValueEncoder, MyRecordIO](ValueEncoderDeriver.summoned)
    implicit val valueDecoder: ValueDecoder[MyRecordIO]   =
      Derive.derive[ValueDecoder, MyRecordIO](ValueDecoderDeriver.summoned)
    implicit val typeTag: TypeTag[MyRecordIO]             =
      Derive.derive[TypeTag, MyRecordIO](TypeTagDeriver.default)
  }

  case class MyProjectedRecordIO(a: Int, c: Option[Long], d: List[Int], e: Map[String, Int])
  object MyProjectedRecordIO {
    implicit val schema: zio.schema.Schema.CaseClass4.WithFields[
      "a",
      "c",
      "d",
      "e",
      Int,
      Option[Long],
      List[Int],
      Map[String, Int],
      MyProjectedRecordIO
    ]                                                              =
      DeriveSchema.gen[MyProjectedRecordIO]
    implicit val schemaEncoder: SchemaEncoder[MyProjectedRecordIO] =
      Derive.derive[SchemaEncoder, MyProjectedRecordIO](SchemaEncoderDeriver.summoned)
    implicit val valueEncoder: ValueEncoder[MyProjectedRecordIO]   =
      Derive.derive[ValueEncoder, MyProjectedRecordIO](ValueEncoderDeriver.summoned)
    implicit val valueDecoder: ValueDecoder[MyProjectedRecordIO]   =
      Derive.derive[ValueDecoder, MyProjectedRecordIO](ValueDecoderDeriver.summoned)
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
    currency: Currency,
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
    implicit val schema: zio.schema.Schema.CaseClass22.WithFields[
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
      "currency",
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
      java.util.Currency,
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
      MyRecordAllTypes1
    ]                                                =
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
      MyRecordAllTypes2
    ]                                                =
      DeriveSchema.gen[MyRecordAllTypes2]
    implicit val typeTag: TypeTag[MyRecordAllTypes2] =
      Derive.derive[TypeTag, MyRecordAllTypes2](TypeTagDeriver.default)
  }

}
