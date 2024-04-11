package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.filter.TypeTag._
import org.apache.parquet.filter2.predicate.FilterApi
import zio._
import zio.test.Assertion._
import zio.test._

import java.time._
import java.util.UUID
import scala.jdk.CollectionConverters._

import Fixtures._

object ExprSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ExprSpec")(
      test("compile all operators") {
        val (a, b, _, _, _) = Filter[MyRecord].columns

        val result = Filter.compile(
          Filter.not(
            (b >= 3 or b <= 100 and a.in(Set("foo", "bar"))) or
              (a === "foo" and (b === 20 or b.notIn(Set(1, 2, 3)))) or
              (a =!= "foo" and b > 2 and b < 10)
          )
        )

        val acol     = FilterApi.binaryColumn("a")
        val bcol     = FilterApi.intColumn("b")
        val expected =
          FilterApi.not(
            FilterApi.or(
              FilterApi.or(
                FilterApi.and(
                  FilterApi.or(
                    FilterApi.gtEq(bcol, Int.box(Value.int(3).value)),
                    FilterApi.ltEq(bcol, Int.box(Value.int(100).value))
                  ),
                  FilterApi.in(acol, Set(Value.string("foo").value, Value.string("bar").value).asJava)
                ),
                FilterApi.and(
                  FilterApi.eq(acol, Value.string("foo").value),
                  FilterApi.or(
                    FilterApi.eq(bcol, Int.box(Value.int(20).value)),
                    FilterApi.notIn(bcol, Set(1, 2, 3).map(i => Int.box(Value.int(i).value)).asJava)
                  )
                )
              ),
              FilterApi.and(
                FilterApi.and(
                  FilterApi.notEq(acol, Value.string("foo").value),
                  FilterApi.gt(bcol, Int.box(Value.int(2).value))
                ),
                FilterApi.lt(bcol, Int.box(Value.int(10).value))
              )
            )
          )

        assert(result)(isRight(equalTo(expected)))
      },
      test("compile summoned") {
        val (a, b) = Filter[MyRecordSummoned].columns

        val result = Filter.compile(
          a === 3 and b === "foo"
        )

        val acol     = FilterApi.binaryColumn("a")
        val bcol     = FilterApi.binaryColumn("b")
        val expected = FilterApi.and(
          FilterApi.eq(acol, Value.string("3").value),
          FilterApi.eq(bcol, Value.string("foo").value)
        )

        assert(result)(isRight(equalTo(expected)))
      },
      test("compile all primitive types") {
        val (
          string,
          boolean,
          byte,
          short,
          int,
          long,
          float,
          double,
          binary,
          char,
          uuid,
          bigDecimal,
          bigInteger,
          dayOfWeek,
          month,
          monthDay,
          period,
          year,
          yearMonth,
          zoneId,
          zoneOffset
        ) = Filter[MyRecordAllTypes1].columns

        val (
          duration,
          instant,
          localDate,
          localTime,
          localDateTime,
          offsetTime,
          offsetDateTime,
          zonedDateTime
        ) = Filter[MyRecordAllTypes2].columns

        val stringPayload         = "foo"
        val booleanPayload        = true
        val bytePayload           = 1.toByte
        val shortPayload          = 1.toShort
        val intPayload            = 1
        val longPayload           = 1L
        val floatPayload          = 1.0f
        val doublePayload         = 1.0
        val binaryPayload         = Chunk(1.toByte, 2.toByte)
        val charPayload           = 'c'
        val uuidPayload           = UUID.randomUUID()
        val bigDecimalPayload     = new java.math.BigDecimal("1.0")
        val bigIntegerPayload     = new java.math.BigInteger("99999999999")
        val dayOfWeekPayload      = DayOfWeek.of(1)
        val monthPayload          = Month.of(1)
        val monthDayPayload       = MonthDay.of(1, 1)
        val periodPayload         = Period.of(1, 1, 1)
        val yearPayload           = Year.of(1)
        val yearMonthPayload      = YearMonth.of(1, 1)
        val zoneIdPayload         = ZoneId.of("Europe/Paris")
        val zoneOffsetPayload     = ZoneOffset.of("+02:00")
        val durationPayload       = 1.second
        val instantPayload        = Instant.ofEpochMilli(1)
        val localDatePayload      = LocalDate.ofEpochDay(1)
        val localTimePayload      = LocalTime.ofInstant(instantPayload, zoneIdPayload)
        val localDateTimePayload  = LocalDateTime.of(localDatePayload, localTimePayload)
        val offsetTimePayload     = OffsetTime.ofInstant(instantPayload, zoneIdPayload)
        val offsetDateTimePayload = OffsetDateTime.ofInstant(instantPayload, zoneIdPayload)
        val zonedDateTimePayload  = ZonedDateTime.ofInstant(localDateTimePayload, zoneOffsetPayload, zoneIdPayload)

        val stringExpected         = FilterApi.eq(
          FilterApi.binaryColumn("string"),
          Value.string(stringPayload).value
        )
        val booleanExpected        = FilterApi.eq(
          FilterApi.booleanColumn("boolean"),
          Boolean.box(Value.boolean(booleanPayload).value)
        )
        val byteExpected           = FilterApi.eq(
          FilterApi.intColumn("byte"),
          Int.box(Value.byte(bytePayload).value)
        )
        val shortExpected          = FilterApi.eq(
          FilterApi.intColumn("short"),
          Int.box(Value.short(shortPayload).value)
        )
        val intExpected            = FilterApi.eq(
          FilterApi.intColumn("int"),
          Int.box(Value.int(intPayload).value)
        )
        val longExpected           = FilterApi.eq(
          FilterApi.longColumn("long"),
          Long.box(Value.long(longPayload).value)
        )
        val floatExpected          = FilterApi.eq(
          FilterApi.floatColumn("float"),
          Float.box(Value.float(floatPayload).value)
        )
        val doubleExpected         = FilterApi.eq(
          FilterApi.doubleColumn("double"),
          Double.box(Value.double(doublePayload).value)
        )
        val binaryExpected         = FilterApi.eq(
          FilterApi.binaryColumn("binary"),
          Value.binary(binaryPayload).value
        )
        val charExpected           = FilterApi.eq(
          FilterApi.intColumn("char"),
          Int.box(Value.char(charPayload).value)
        )
        val uuidExpected           = FilterApi.eq(
          FilterApi.binaryColumn("uuid"),
          Value.uuid(uuidPayload).value
        )
        val bigDecimalExpected     = FilterApi.eq(
          FilterApi.longColumn("bigDecimal"),
          Long.box(Value.bigDecimal(bigDecimalPayload).value)
        )
        val bigIntegerExpected     = FilterApi.eq(
          FilterApi.binaryColumn("bigInteger"),
          Value.bigInteger(bigIntegerPayload).value
        )
        val dayOfWeekExpected      = FilterApi.eq(
          FilterApi.intColumn("dayOfWeek"),
          Int.box(Value.dayOfWeek(dayOfWeekPayload).value)
        )
        val monthExpected          = FilterApi.eq(
          FilterApi.intColumn("month"),
          Int.box(Value.month(monthPayload).value)
        )
        val monthDayExpected       = FilterApi.eq(
          FilterApi.binaryColumn("monthDay"),
          Value.monthDay(monthDayPayload).value
        )
        val periodExpected         = FilterApi.eq(
          FilterApi.binaryColumn("period"),
          Value.period(periodPayload).value
        )
        val yearExpected           = FilterApi.eq(
          FilterApi.intColumn("year"),
          Int.box(Value.year(yearPayload).value)
        )
        val yearMonthExpected      = FilterApi.eq(
          FilterApi.binaryColumn("yearMonth"),
          Value.yearMonth(yearMonthPayload).value
        )
        val zoneIdExpected         = FilterApi.eq(
          FilterApi.binaryColumn("zoneId"),
          Value.zoneId(zoneIdPayload).value
        )
        val zoneOffsetExpected     = FilterApi.eq(
          FilterApi.binaryColumn("zoneOffset"),
          Value.zoneOffset(zoneOffsetPayload).value
        )
        val durationExpected       = FilterApi.eq(
          FilterApi.longColumn("duration"),
          Long.box(Value.duration(durationPayload).value)
        )
        val instantExpected        = FilterApi.eq(
          FilterApi.longColumn("instant"),
          Long.box(Value.instant(instantPayload).value)
        )
        val localDateExpected      = FilterApi.eq(
          FilterApi.intColumn("localDate"),
          Int.box(Value.localDate(localDatePayload).value)
        )
        val localTimeExpected      = FilterApi.eq(
          FilterApi.intColumn("localTime"),
          Int.box(Value.localTime(localTimePayload).value)
        )
        val localDateTimeExpected  = FilterApi.eq(
          FilterApi.longColumn("localDateTime"),
          Long.box(Value.localDateTime(localDateTimePayload).value)
        )
        val offsetTimeExpected     = FilterApi.eq(
          FilterApi.intColumn("offsetTime"),
          Int.box(Value.offsetTime(offsetTimePayload).value)
        )
        val offsetDateTimeExpected = FilterApi.eq(
          FilterApi.longColumn("offsetDateTime"),
          Long.box(Value.offsetDateTime(offsetDateTimePayload).value)
        )
        val zonedDateTimeExpected  = FilterApi.eq(
          FilterApi.longColumn("zonedDateTime"),
          Long.box(Value.zonedDateTime(zonedDateTimePayload).value)
        )

        ZIO.fromEither(
          for {
            stringResul          <- Filter.compile(string === stringPayload)
            booleanResult        <- Filter.compile(boolean === booleanPayload)
            byteResult           <- Filter.compile(byte === bytePayload)
            shortResult          <- Filter.compile(short === shortPayload)
            intResult            <- Filter.compile(int === intPayload)
            longResult           <- Filter.compile(long === longPayload)
            floatResult          <- Filter.compile(float === floatPayload)
            doubleResult         <- Filter.compile(double === doublePayload)
            binaryResult         <- Filter.compile(binary === binaryPayload)
            charResult           <- Filter.compile(char === charPayload)
            uuidResult           <- Filter.compile(uuid === uuidPayload)
            bigDecimalResult     <- Filter.compile(bigDecimal === bigDecimalPayload)
            bigIntegerResult     <- Filter.compile(bigInteger === bigIntegerPayload)
            dayOfWeekResult      <- Filter.compile(dayOfWeek === dayOfWeekPayload)
            monthResult          <- Filter.compile(month === monthPayload)
            monthDayResult       <- Filter.compile(monthDay === monthDayPayload)
            periodResult         <- Filter.compile(period === periodPayload)
            yearResult           <- Filter.compile(year === yearPayload)
            yearMonthResult      <- Filter.compile(yearMonth === yearMonthPayload)
            zoneIdResult         <- Filter.compile(zoneId === zoneIdPayload)
            zoneOffsetResult     <- Filter.compile(zoneOffset === zoneOffsetPayload)
            durationResult       <- Filter.compile(duration === durationPayload)
            instantResult        <- Filter.compile(instant === instantPayload)
            localDateResult      <- Filter.compile(localDate === localDatePayload)
            localTimeResult      <- Filter.compile(localTime === localTimePayload)
            localDateTimeResult  <- Filter.compile(localDateTime === localDateTimePayload)
            offsetTimeResult     <- Filter.compile(offsetTime === offsetTimePayload)
            offsetDateTimeResult <- Filter.compile(offsetDateTime === offsetDateTimePayload)
            zonedDateTimeResult  <- Filter.compile(zonedDateTime === zonedDateTimePayload)
          } yield assertTrue(
            stringResul == stringExpected,
            booleanResult == booleanExpected,
            byteResult == byteExpected,
            shortResult == shortExpected,
            intResult == intExpected,
            longResult == longExpected,
            floatResult == floatExpected,
            doubleResult == doubleExpected,
            binaryResult == binaryExpected,
            charResult == charExpected,
            uuidResult == uuidExpected,
            bigDecimalResult == bigDecimalExpected,
            bigIntegerResult == bigIntegerExpected,
            dayOfWeekResult == dayOfWeekExpected,
            monthResult == monthExpected,
            monthDayResult == monthDayExpected,
            periodResult == periodExpected,
            yearResult == yearExpected,
            yearMonthResult == yearMonthExpected,
            zoneIdResult == zoneIdExpected,
            zoneOffsetResult == zoneOffsetExpected,
            durationResult == durationExpected,
            instantResult == instantExpected,
            localDateResult == localDateExpected,
            localTimeResult == localTimeExpected,
            localDateTimeResult == localDateTimeExpected,
            offsetTimeResult == offsetTimeExpected,
            offsetDateTimeResult == offsetDateTimeExpected,
            zonedDateTimeResult == zonedDateTimeExpected
          )
        )
      },
      test("compile option") {
        // TODO: test failing compile-time cases
        val (_, _, _, _, opt) = Filter[MyRecord].columns

        val expected = FilterApi.gt(FilterApi.intColumn("opt"), Int.box(Value.int(3).value))
        val result   = compile(opt.nullable > 3)

        assert(result)(isRight(equalTo(expected)))
      },
      test("compile enum") {
        val (_, _, _, enm, _) = Filter[MyRecord].columns

        val result   = Filter.compile(enm === MyRecord.Enum.Done)
        val expected = FilterApi.eq(FilterApi.binaryColumn("enm"), Value.string("Done").value)

        assert(result)(isRight(equalTo(expected)))
      },
      test("column path concatenation") {
        // TODO: test failing compile-time cases
        // Show the macro determines the names of the parent/child fields no matter how we name
        // the variables that represent columns
        val (_, _, child0, _, _) = Filter[MyRecord].columns
        val (c0, d0)             = Filter[MyRecord.Child].columns

        assert(concat(child0, c0).path)(equalTo("child.c")) &&
        assert(concat(child0, d0).path)(equalTo("child.d"))
      }
    )

}
