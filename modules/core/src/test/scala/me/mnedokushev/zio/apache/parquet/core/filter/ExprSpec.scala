package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Fixtures._
import me.mnedokushev.zio.apache.parquet.core.Value
import me.mnedokushev.zio.apache.parquet.core.filter.TypeTag._
import me.mnedokushev.zio.apache.parquet.core.filter.syntax._
import org.apache.parquet.filter2.predicate.FilterApi
import zio._
import zio.test.Assertion.{ equalTo, isRight }
import zio.test._

import java.time._
import java.util.{Currency, UUID}
import scala.jdk.CollectionConverters._

object ExprSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ExprSpec")(
      test("compile all operators") {
        val (a, b, _, _, _) = Filter[MyRecord].columns

        val result = filter(
          not(
            (b >= 3 `or` b <= 100 `and` a.in(Set("foo", "bar"))) `or`
              (a === "foo" `and` (b === 20 `or` b.notIn(Set(1, 2, 3)))) `or`
              (a =!= "foo" `and` b > 2 `and` b < 10)
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

        val result = filter(
          a === 3 `and` b === "foo"
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
          currency,
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
        val currencyPayload       = Currency.getInstance("USD")
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
        val currencyExpected       = FilterApi.eq(
          FilterApi.binaryColumn("currency"),
          Value.currency(currencyPayload).value
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

        val stringResul          = filter(string === stringPayload)
        val booleanResult        = filter(boolean === booleanPayload)
        val byteResult           = filter(byte === bytePayload)
        val shortResult          = filter(short === shortPayload)
        val intResult            = filter(int === intPayload)
        val longResult           = filter(long === longPayload)
        val floatResult          = filter(float === floatPayload)
        val doubleResult         = filter(double === doublePayload)
        val binaryResult         = filter(binary === binaryPayload)
        val charResult           = filter(char === charPayload)
        val uuidResult           = filter(uuid === uuidPayload)
        val currencyResult       = filter(currency === currencyPayload)
        val bigDecimalResult     = filter(bigDecimal === bigDecimalPayload)
        val bigIntegerResult     = filter(bigInteger === bigIntegerPayload)
        val dayOfWeekResult      = filter(dayOfWeek === dayOfWeekPayload)
        val monthResult          = filter(month === monthPayload)
        val monthDayResult       = filter(monthDay === monthDayPayload)
        val periodResult         = filter(period === periodPayload)
        val yearResult           = filter(year === yearPayload)
        val yearMonthResult      = filter(yearMonth === yearMonthPayload)
        val zoneIdResult         = filter(zoneId === zoneIdPayload)
        val zoneOffsetResult     = filter(zoneOffset === zoneOffsetPayload)
        val durationResult       = filter(duration === durationPayload)
        val instantResult        = filter(instant === instantPayload)
        val localDateResult      = filter(localDate === localDatePayload)
        val localTimeResult      = filter(localTime === localTimePayload)
        val localDateTimeResult  = filter(localDateTime === localDateTimePayload)
        val offsetTimeResult     = filter(offsetTime === offsetTimePayload)
        val offsetDateTimeResult = filter(offsetDateTime === offsetDateTimePayload)
        val zonedDateTimeResult  = filter(zonedDateTime === zonedDateTimePayload)

        assert(stringResul)(isRight(equalTo(stringExpected))) &&
        assert(booleanResult)(isRight(equalTo(booleanExpected))) &&
        assert(byteResult)(isRight(equalTo(byteExpected))) &&
        assert(shortResult)(isRight(equalTo(shortExpected))) &&
        assert(intResult)(isRight(equalTo(intExpected))) &&
        assert(longResult)(isRight(equalTo(longExpected))) &&
        assert(floatResult)(isRight(equalTo(floatExpected))) &&
        assert(doubleResult)(isRight(equalTo(doubleExpected))) &&
        assert(binaryResult)(isRight(equalTo(binaryExpected))) &&
        assert(charResult)(isRight(equalTo(charExpected))) &&
        assert(uuidResult)(isRight(equalTo(uuidExpected))) &&
        assert(currencyResult)(isRight(equalTo(currencyExpected))) &&
        assert(bigDecimalResult)(isRight(equalTo(bigDecimalExpected))) &&
        assert(bigIntegerResult)(isRight(equalTo(bigIntegerExpected))) &&
        assert(dayOfWeekResult)(isRight(equalTo(dayOfWeekExpected))) &&
        assert(monthResult)(isRight(equalTo(monthExpected))) &&
        assert(monthDayResult)(isRight(equalTo(monthDayExpected))) &&
        assert(periodResult)(isRight(equalTo(periodExpected))) &&
        assert(yearResult)(isRight(equalTo(yearExpected))) &&
        assert(yearMonthResult)(isRight(equalTo(yearMonthExpected))) &&
        assert(zoneIdResult)(isRight(equalTo(zoneIdExpected))) &&
        assert(zoneOffsetResult)(isRight(equalTo(zoneOffsetExpected))) &&
        assert(durationResult)(isRight(equalTo(durationExpected))) &&
        assert(instantResult)(isRight(equalTo(instantExpected))) &&
        assert(localDateResult)(isRight(equalTo(localDateExpected))) &&
        assert(localTimeResult)(isRight(equalTo(localTimeExpected))) &&
        assert(localDateTimeResult)(isRight(equalTo(localDateTimeExpected))) &&
        assert(offsetTimeResult)(isRight(equalTo(offsetTimeExpected))) &&
        assert(offsetDateTimeResult)(isRight(equalTo(offsetDateTimeExpected))) &&
        assert(zonedDateTimeResult)(isRight(equalTo(zonedDateTimeExpected)))
      },
      test("compile option") {
        // TODO: test failing compile-time cases
        val (_, _, _, _, opt) = Filter[MyRecord].columns

        val expected = FilterApi.gt(FilterApi.intColumn("opt"), Int.box(Value.int(3).value))
        val result   = filter(opt.nullable > 3)

        assert(result)(isRight(equalTo(expected)))
      },
      test("compile enum") {
        val (_, _, _, enm, _) = Filter[MyRecord].columns

        val result   = filter(enm === MyRecord.Enum.Done)
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
