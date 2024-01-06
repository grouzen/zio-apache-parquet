package me.mnedokushev.zio.apache.parquet.core.codec

//import me.mnedokushev.zio.apache.parquet.core.Value
//import me.mnedokushev.zio.apache.parquet.core.Value.PrimitiveValue
import me.mnedokushev.zio.apache.parquet.core.{ NANOS_FACTOR, NANOS_PER_DAY, Value }
import zio._
import zio.schema._
import zio.test.Assertion._
import zio.test._

import java.math.{ BigDecimal, BigInteger }
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

//import java.nio.ByteBuffer
//import java.util.UUID
//import scala.annotation.nowarn

object ValueCodecDeriverSpec extends ZIOSpecDefault {

  sealed trait MyEnum
  object MyEnum {
    case object Started    extends MyEnum
    case object InProgress extends MyEnum
    case object Done       extends MyEnum

    implicit val schema: Schema[MyEnum] = DeriveSchema.gen[MyEnum]
  }

  case class Record(a: Int, b: Boolean, c: Option[String], d: List[Int], e: Map[String, Int])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  case class SummonedRecord(a: Int, b: Boolean, c: Option[String], d: Option[Int])
  object SummonedRecord {
    implicit val schema: Schema[SummonedRecord] = DeriveSchema.gen[SummonedRecord]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ValueCodecDeriverSpec")(
      test("primitive") {
        val stringEncoder         = Derive.derive[ValueEncoder, String](ValueEncoderDeriver.default)
        val booleanEncoder        = Derive.derive[ValueEncoder, Boolean](ValueEncoderDeriver.default)
        val byteEncoder           = Derive.derive[ValueEncoder, Byte](ValueEncoderDeriver.default)
        val shortEncoder          = Derive.derive[ValueEncoder, Short](ValueEncoderDeriver.default)
        val intEncoder            = Derive.derive[ValueEncoder, Int](ValueEncoderDeriver.default)
        val longEncoder           = Derive.derive[ValueEncoder, Long](ValueEncoderDeriver.default)
        val floatEncoder          = Derive.derive[ValueEncoder, Float](ValueEncoderDeriver.default)
        val doubleEncoder         = Derive.derive[ValueEncoder, Double](ValueEncoderDeriver.default)
        val binaryEncoder         = Derive.derive[ValueEncoder, Chunk[Byte]](ValueEncoderDeriver.default)
        val charEncoder           = Derive.derive[ValueEncoder, Char](ValueEncoderDeriver.default)
        val uuidEncoder           = Derive.derive[ValueEncoder, UUID](ValueEncoderDeriver.default)
        val bigDecimalEncoder     = Derive.derive[ValueEncoder, BigDecimal](ValueEncoderDeriver.default)
        val bigIntegerEncoder     = Derive.derive[ValueEncoder, BigInteger](ValueEncoderDeriver.default)
        val dayOfWeekEncoder      = Derive.derive[ValueEncoder, DayOfWeek](ValueEncoderDeriver.default)
        val monthEncoder          = Derive.derive[ValueEncoder, Month](ValueEncoderDeriver.default)
        val monthDayEncoder       = Derive.derive[ValueEncoder, MonthDay](ValueEncoderDeriver.default)
        val periodEncoder         = Derive.derive[ValueEncoder, Period](ValueEncoderDeriver.default)
        val yearEncoder           = Derive.derive[ValueEncoder, Year](ValueEncoderDeriver.default)
        val yearMonthEncoder      = Derive.derive[ValueEncoder, YearMonth](ValueEncoderDeriver.default)
        val zoneIdEncoder         = Derive.derive[ValueEncoder, ZoneId](ValueEncoderDeriver.default)
        val zoneOffsetEncoder     = Derive.derive[ValueEncoder, ZoneOffset](ValueEncoderDeriver.default)
        val durationEncoder       = Derive.derive[ValueEncoder, Duration](ValueEncoderDeriver.default)
        val instantEncoder        = Derive.derive[ValueEncoder, Instant](ValueEncoderDeriver.default)
        val localDateEncoder      = Derive.derive[ValueEncoder, LocalDate](ValueEncoderDeriver.default)
        val localTimeEncoder      = Derive.derive[ValueEncoder, LocalTime](ValueEncoderDeriver.default)
        val localDateTimeEncoder  = Derive.derive[ValueEncoder, LocalDateTime](ValueEncoderDeriver.default)
        val offsetTimeEncoder     = Derive.derive[ValueEncoder, OffsetTime](ValueEncoderDeriver.default)
        val offsetDateTimeEncoder = Derive.derive[ValueEncoder, OffsetDateTime](ValueEncoderDeriver.default)
        val zonedDateTimeEncoder  = Derive.derive[ValueEncoder, ZonedDateTime](ValueEncoderDeriver.default)

        val stringDecoder         = Derive.derive[ValueDecoder, String](ValueDecoderDeriver.default)
        val booleanDecoder        = Derive.derive[ValueDecoder, Boolean](ValueDecoderDeriver.default)
        val byteDecoder           = Derive.derive[ValueDecoder, Byte](ValueDecoderDeriver.default)
        val shortDecoder          = Derive.derive[ValueDecoder, Short](ValueDecoderDeriver.default)
        val intDecoder            = Derive.derive[ValueDecoder, Int](ValueDecoderDeriver.default)
        val longDecoder           = Derive.derive[ValueDecoder, Long](ValueDecoderDeriver.default)
        val floatDecoder          = Derive.derive[ValueDecoder, Float](ValueDecoderDeriver.default)
        val doubleDecoder         = Derive.derive[ValueDecoder, Double](ValueDecoderDeriver.default)
        val binaryDecoder         = Derive.derive[ValueDecoder, Chunk[Byte]](ValueDecoderDeriver.default)
        val charDecoder           = Derive.derive[ValueDecoder, Char](ValueDecoderDeriver.default)
        val uuidDecoder           = Derive.derive[ValueDecoder, UUID](ValueDecoderDeriver.default)
        val bigDecimalDecoder     = Derive.derive[ValueDecoder, BigDecimal](ValueDecoderDeriver.default)
        val bigIntegerDecoder     = Derive.derive[ValueDecoder, BigInteger](ValueDecoderDeriver.default)
        val dayOfWeekDecoder      = Derive.derive[ValueDecoder, DayOfWeek](ValueDecoderDeriver.default)
        val monthDecoder          = Derive.derive[ValueDecoder, Month](ValueDecoderDeriver.default)
        val monthDayDecoder       = Derive.derive[ValueDecoder, MonthDay](ValueDecoderDeriver.default)
        val periodDecoder         = Derive.derive[ValueDecoder, Period](ValueDecoderDeriver.default)
        val yearDecoder           = Derive.derive[ValueDecoder, Year](ValueDecoderDeriver.default)
        val yearMonthDecoder      = Derive.derive[ValueDecoder, YearMonth](ValueDecoderDeriver.default)
        val zoneIdDecoder         = Derive.derive[ValueDecoder, ZoneId](ValueDecoderDeriver.default)
        val zoneOffsetDecoder     = Derive.derive[ValueDecoder, ZoneOffset](ValueDecoderDeriver.default)
        val durationDecoder       = Derive.derive[ValueDecoder, Duration](ValueDecoderDeriver.default)
        val instantDecoder        = Derive.derive[ValueDecoder, Instant](ValueDecoderDeriver.default)
        val localDateDecoder      = Derive.derive[ValueDecoder, LocalDate](ValueDecoderDeriver.default)
        val localTimeDecoder      = Derive.derive[ValueDecoder, LocalTime](ValueDecoderDeriver.default)
        val localDateTimeDecoder  = Derive.derive[ValueDecoder, LocalDateTime](ValueDecoderDeriver.default)
        val offsetTimeDecoder     = Derive.derive[ValueDecoder, OffsetTime](ValueDecoderDeriver.default)
        val offsetDateTimeDecoder = Derive.derive[ValueDecoder, OffsetDateTime](ValueDecoderDeriver.default)
        val zonedDateTimeDecoder  = Derive.derive[ValueDecoder, ZonedDateTime](ValueDecoderDeriver.default)

        val stringPayload         = "foo"
        val booleanPayload        = false
        val bytePayload           = 10.toByte
        val shortPayload          = 30.toShort
        val intPayload            = 254
        val longPayload           = 398812L
        val floatPayload          = 23.123f
        val doublePayload         = 0.1231454123d
        val binaryPayload         = Chunk.fromArray("foobar".getBytes)
        val charPayload           = 'c'
        val uuidPayload           = UUID.randomUUID()
        val bigDecimalPayload     = new BigDecimal("993123312.32")
        val bigIntegerPayload     = new BigInteger("99931234123")
        val dayOfWeekPayload      = DayOfWeek.of(5)
        val monthPayload          = Month.of(12)
        val monthDayPayload       = MonthDay.of(10, 23)
        val periodPayload         = Period.of(9, 1, 14)
        val yearPayload           = Year.of(1989)
        val yearMonthPayload      = YearMonth.of(1989, 5)
        val zoneIdPayload         = ZoneId.of("Europe/Paris")
        val zoneOffsetPayload     = ZoneOffset.of("+02:00")
        val durationPayload       = Duration.fromSeconds(99312312)
        val instantPayload        = Instant.ofEpochMilli(Instant.now().toEpochMilli)
        val localDatePayload      = LocalDate.now()
        val localTimePayload      = LocalTime.ofNanoOfDay((LocalTime.now().toNanoOfDay / 1000000L) * 1000000L)
        val localDateTimePayload  = LocalDateTime.of(localDatePayload, localTimePayload)
        val offsetTimePayload     = OffsetTime.of(localTimePayload, zoneOffsetPayload)
        val offsetDateTimePayload = OffsetDateTime.of(localDateTimePayload, zoneOffsetPayload)
        val zonedDateTimePayload  = ZonedDateTime.of(localDateTimePayload, zoneIdPayload)

        // TODO: fails when the current time is after midnight
        val expectedOffsetTimeUTC = {
          val timeNanos       = offsetTimePayload.toLocalTime.toNanoOfDay
          val offsetNanos     = offsetTimePayload.getOffset.getTotalSeconds * NANOS_FACTOR
          val timeOffsetNanos = timeNanos - offsetNanos
          val nanoOfDay        = if(timeOffsetNanos < 0) NANOS_PER_DAY - timeOffsetNanos else timeOffsetNanos

          OffsetTime.of(LocalTime.ofNanoOfDay(nanoOfDay), ZoneOffset.UTC)
        }

        val expectedOffsetDateTimeUTC = {
          val epochDay            = offsetDateTimePayload.toLocalDate.toEpochDay
          val timeNanos           = offsetDateTimePayload.toLocalTime.toNanoOfDay
          val offsetNanos         = offsetDateTimePayload.getOffset.getTotalSeconds * NANOS_FACTOR
          val timeOffsetNanos     = timeNanos - offsetNanos
          val nanoOfDay           = if (timeOffsetNanos < 0) NANOS_PER_DAY - timeOffsetNanos else timeOffsetNanos
          val epochDayOrDayBefore = if (timeOffsetNanos < 0) epochDay - 1 else epochDay

          OffsetDateTime.of(
            LocalDateTime.of(LocalDate.ofEpochDay(epochDayOrDayBefore), LocalTime.ofNanoOfDay(nanoOfDay)),
            ZoneOffset.UTC
          )
        }

        val expectedZonedDateTimeUTC = {
          val epochDay            = zonedDateTimePayload.toLocalDate.toEpochDay
          val timeNanos           = zonedDateTimePayload.toLocalTime.toNanoOfDay
          val offsetNanos         = zonedDateTimePayload.getOffset.getTotalSeconds * NANOS_FACTOR
          val timeOffsetNanos     = timeNanos - offsetNanos
          val nanoOfDay           = if (timeOffsetNanos < 0) NANOS_PER_DAY - timeOffsetNanos else timeOffsetNanos
          val epochDayOrDayBefore = if (timeOffsetNanos < 0) epochDay - 1 else epochDay

          ZonedDateTime.of(
            LocalDateTime.of(LocalDate.ofEpochDay(epochDayOrDayBefore), LocalTime.ofNanoOfDay(nanoOfDay)),
            ZoneId.of("Z")
          )
        }

        for {
          stringValue          <- stringEncoder.encodeZIO(stringPayload)
          stringResult         <- stringDecoder.decodeZIO(stringValue)
          booleanValue         <- booleanEncoder.encodeZIO(booleanPayload)
          booleanResult        <- booleanDecoder.decodeZIO(booleanValue)
          byteValue            <- byteEncoder.encodeZIO(bytePayload)
          byteResult           <- byteDecoder.decodeZIO(byteValue)
          shortValue           <- shortEncoder.encodeZIO(shortPayload)
          shortResult          <- shortDecoder.decodeZIO(shortValue)
          intValue             <- intEncoder.encodeZIO(intPayload)
          intResult            <- intDecoder.decodeZIO(intValue)
          longValue            <- longEncoder.encodeZIO(longPayload)
          longResult           <- longDecoder.decodeZIO(longValue)
          floatValue           <- floatEncoder.encodeZIO(floatPayload)
          floatResult          <- floatDecoder.decodeZIO(floatValue)
          doubleValue          <- doubleEncoder.encodeZIO(doublePayload)
          doubleResult         <- doubleDecoder.decodeZIO(doubleValue)
          binaryValue          <- binaryEncoder.encodeZIO(binaryPayload)
          binaryResult         <- binaryDecoder.decodeZIO(binaryValue)
          charValue            <- charEncoder.encodeZIO(charPayload)
          charResult           <- charDecoder.decodeZIO(charValue)
          uuidValue            <- uuidEncoder.encodeZIO(uuidPayload)
          uuidResult           <- uuidDecoder.decodeZIO(uuidValue)
          bigDecimalValue      <- bigDecimalEncoder.encodeZIO(bigDecimalPayload)
          bigDecimalResult     <- bigDecimalDecoder.decodeZIO(bigDecimalValue)
          bigIntegerValue      <- bigIntegerEncoder.encodeZIO(bigIntegerPayload)
          bigIntegerResult     <- bigIntegerDecoder.decodeZIO(bigIntegerValue)
          dayOfWeekValue       <- dayOfWeekEncoder.encodeZIO(dayOfWeekPayload)
          dayOfWeekResult      <- dayOfWeekDecoder.decodeZIO(dayOfWeekValue)
          monthValue           <- monthEncoder.encodeZIO(monthPayload)
          monthResult          <- monthDecoder.decodeZIO(monthValue)
          monthDayValue        <- monthDayEncoder.encodeZIO(monthDayPayload)
          monthDayResult       <- monthDayDecoder.decodeZIO(monthDayValue)
          periodValue          <- periodEncoder.encodeZIO(periodPayload)
          periodResult         <- periodDecoder.decodeZIO(periodValue)
          yearValue            <- yearEncoder.encodeZIO(yearPayload)
          yearResult           <- yearDecoder.decodeZIO(yearValue)
          yearMonthValue       <- yearMonthEncoder.encodeZIO(yearMonthPayload)
          yearMonthResult      <- yearMonthDecoder.decodeZIO(yearMonthValue)
          zoneIdValue          <- zoneIdEncoder.encodeZIO(zoneIdPayload)
          zoneIdResult         <- zoneIdDecoder.decodeZIO(zoneIdValue)
          zoneOffsetValue      <- zoneOffsetEncoder.encodeZIO(zoneOffsetPayload)
          zoneOffsetResult     <- zoneOffsetDecoder.decodeZIO(zoneOffsetValue)
          durationValue        <- durationEncoder.encodeZIO(durationPayload)
          durationResult       <- durationDecoder.decodeZIO(durationValue)
          instantValue         <- instantEncoder.encodeZIO(instantPayload)
          instantResult        <- instantDecoder.decodeZIO(instantValue)
          localDateValue       <- localDateEncoder.encodeZIO(localDatePayload)
          localDateResult      <- localDateDecoder.decodeZIO(localDateValue)
          localTimeValue       <- localTimeEncoder.encodeZIO(localTimePayload)
          localTimeResult      <- localTimeDecoder.decodeZIO(localTimeValue)
          localDateTimeValue   <- localDateTimeEncoder.encodeZIO(localDateTimePayload)
          localDateTimeResult  <- localDateTimeDecoder.decodeZIO(localDateTimeValue)
          offsetTimeValue      <- offsetTimeEncoder.encodeZIO(offsetTimePayload)
          offsetTimeResult     <- offsetTimeDecoder.decodeZIO(offsetTimeValue)
          offsetDateTimeValue  <- offsetDateTimeEncoder.encodeZIO(offsetDateTimePayload)
          offsetDateTimeResult <- offsetDateTimeDecoder.decodeZIO(offsetDateTimeValue)
          zonedDateTimeValue   <- zonedDateTimeEncoder.encodeZIO(zonedDateTimePayload)
          zonedDateTimeResult  <- zonedDateTimeDecoder.decodeZIO(zonedDateTimeValue)
        } yield assertTrue(
          stringResult == stringPayload,
          booleanResult == booleanPayload,
          byteResult == bytePayload,
          shortResult == shortPayload,
          intResult == intPayload,
          longResult == longPayload,
          floatResult == floatPayload,
          doubleResult == doublePayload,
          binaryResult == binaryPayload,
          charResult == charPayload,
          uuidResult == uuidPayload,
          bigDecimalResult == bigDecimalPayload,
          bigIntegerResult == bigIntegerPayload,
          dayOfWeekResult == dayOfWeekPayload,
          monthResult == monthPayload,
          monthDayResult == monthDayPayload,
          periodResult == periodPayload,
          yearResult == yearPayload,
          yearMonthResult == yearMonthPayload,
          zoneIdResult == zoneIdPayload,
          zoneOffsetResult == zoneOffsetPayload,
          durationResult == durationPayload,
          instantResult == instantPayload,
          localDateResult == localDatePayload,
          localTimeResult == localTimePayload,
          localDateTimeResult == localDateTimePayload,
          offsetTimeResult == expectedOffsetTimeUTC,
          offsetDateTimeResult == expectedOffsetDateTimeUTC,
          zonedDateTimeResult == expectedZonedDateTimeUTC
        )
      },
      test("option") {
        val encoder  = Derive.derive[ValueEncoder, Option[Int]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, Option[Int]](ValueDecoderDeriver.default)
        val payloads = List(Option(3), Option.empty)

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("sequence") {
        val encoder  = Derive.derive[ValueEncoder, List[Map[String, Int]]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, List[Map[String, Int]]](ValueDecoderDeriver.default)
        val payloads = List(List(Map("foo" -> 1, "bar" -> 2)), List.empty)

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("map") {
        val encoder  = Derive.derive[ValueEncoder, Map[String, Int]](ValueEncoderDeriver.default)
        val decoder  = Derive.derive[ValueDecoder, Map[String, Int]](ValueDecoderDeriver.default)
        val payloads = List(Map("foo" -> 3), Map.empty[String, Int])

        payloads.map { payload =>
          for {
            value  <- encoder.encodeZIO(payload)
            result <- decoder.decodeZIO(value)
          } yield assertTrue(result == payload)
        }.reduce(_ && _)
      },
      test("record") {
        val encoder = Derive.derive[ValueEncoder, Record](ValueEncoderDeriver.default)
        val decoder = Derive.derive[ValueDecoder, Record](ValueDecoderDeriver.default)
        val payload = Record(2, false, Some("data"), List(1), Map("zio" -> 1))

        for {
          value  <- encoder.encodeZIO(payload)
          result <- decoder.decodeZIO(value)
        } yield assertTrue(result == payload)
      },
      test("enum") {
        val encoder = Derive.derive[ValueEncoder, MyEnum](ValueEncoderDeriver.default)
        val decoder = Derive.derive[ValueDecoder, MyEnum](ValueDecoderDeriver.default)
        val payload = MyEnum.Started
        val wrongId = Value.string("Gone")

        for {
          value       <- encoder.encodeZIO(payload)
          result      <- decoder.decodeZIO(value)
          wrongResult <- decoder.decodeZIO(wrongId).either
        } yield assertTrue(result == payload) &&
          assert(wrongResult)(isLeft(isSubtype[DecoderError](anything)))
      }
//      test("summoned") {
      //        @nowarn
      //        implicit val stringEncoder: ValueEncoder[String] = new ValueEncoder[String] {
      //          override def encode(value: String): Value =
      //            Value.uuid(UUID.fromString(value))
      //        }
      //        @nowarn
      //        implicit val stringDecoder: ValueDecoder[String] = new ValueDecoder[String] {
      //          override def decode(value: Value): String =
      //            value match {
      //              case PrimitiveValue.ByteArrayValue(v) if v.length == 16 =>
      //                val bb = ByteBuffer.wrap(v.getBytes)
      //                new UUID(bb.getLong, bb.getLong).toString
      //              case other                                              =>
      //                throw DecoderError(s"Wrong value: $other")
      //
      //            }
      //        }
      //
      //        val uuid    = UUID.randomUUID()
      //        val encoder = Derive.derive[ValueEncoder, SummonedRecord](ValueEncoderDeriver.summoned)
      //        val decoder = Derive.derive[ValueDecoder, SummonedRecord](ValueDecoderDeriver.summoned)
      //        val payload = SummonedRecord(2, false, Some(uuid.toString), None)
      //
      //        for {
      //          value  <- encoder.encodeZIO(payload)
      //          result <- decoder.decodeZIO(value)
      //        } yield assertTrue(result == payload)
      //      }
    )

}
