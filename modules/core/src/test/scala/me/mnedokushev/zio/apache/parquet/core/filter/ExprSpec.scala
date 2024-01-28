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
        val (a, b, _, _, _) = Filter.columns[MyRecord]

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
        val (a, b) = Filter.columns[MyRecordSummoned]

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
        ) = Filter.columns[MyRecordAllTypes1]

        val (
          duration,
          instant,
          localDate,
          localTime,
          localDateTime,
          offsetTime,
          offsetDateTime,
          zonedDateTime
        ) = Filter.columns[MyRecordAllTypes2]

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

        val result1   = Filter.compile(
          string === stringPayload or
            boolean === booleanPayload or
            byte === bytePayload or
            short === shortPayload or
            int === intPayload or
            long === longPayload or
            float === floatPayload or
            double === doublePayload or
            binary === binaryPayload or
            char === charPayload or
            uuid === uuidPayload or
            bigDecimal === bigDecimalPayload or
            bigInteger === bigIntegerPayload or
            dayOfWeek === dayOfWeekPayload or
            month === monthPayload or
            monthDay === monthDayPayload or
            period === periodPayload or
            year === yearPayload or
            yearMonth === yearMonthPayload or
            zoneId === zoneIdPayload or
            zoneOffset === zoneOffsetPayload
        )
        val expected1 =
          FilterApi.or(
            FilterApi.or(
              FilterApi.or(
                FilterApi.or(
                  FilterApi.or(
                    FilterApi.or(
                      FilterApi.or(
                        FilterApi.or(
                          FilterApi.or(
                            FilterApi.or(
                              FilterApi.or(
                                FilterApi.or(
                                  FilterApi.or(
                                    FilterApi.or(
                                      FilterApi.or(
                                        FilterApi.or(
                                          FilterApi.or(
                                            FilterApi.or(
                                              FilterApi.or(
                                                FilterApi.or(
                                                  FilterApi
                                                    .eq(
                                                      FilterApi.binaryColumn("string"),
                                                      Value.string(stringPayload).value
                                                    ),
                                                  FilterApi
                                                    .eq(
                                                      FilterApi.booleanColumn("boolean"),
                                                      Boolean.box(Value.boolean(booleanPayload).value)
                                                    )
                                                ),
                                                FilterApi
                                                  .eq(
                                                    FilterApi.intColumn("byte"),
                                                    Int.box(Value.byte(bytePayload).value)
                                                  )
                                              ),
                                              FilterApi
                                                .eq(
                                                  FilterApi.intColumn("short"),
                                                  Int.box(Value.short(shortPayload).value)
                                                )
                                            ),
                                            FilterApi
                                              .eq(FilterApi.intColumn("int"), Int.box(Value.int(intPayload).value))
                                          ),
                                          FilterApi
                                            .eq(FilterApi.longColumn("long"), Long.box(Value.long(longPayload).value))
                                        ),
                                        FilterApi
                                          .eq(
                                            FilterApi.floatColumn("float"),
                                            Float.box(Value.float(floatPayload).value)
                                          )
                                      ),
                                      FilterApi
                                        .eq(
                                          FilterApi.doubleColumn("double"),
                                          Double.box(Value.double(doublePayload).value)
                                        )
                                    ),
                                    FilterApi.eq(FilterApi.binaryColumn("binary"), Value.binary(binaryPayload).value)
                                  ),
                                  FilterApi.eq(FilterApi.intColumn("char"), Int.box(Value.char(charPayload).value))
                                ),
                                FilterApi.eq(FilterApi.binaryColumn("uuid"), Value.uuid(uuidPayload).value)
                              ),
                              FilterApi
                                .eq(
                                  FilterApi.longColumn("bigDecimal"),
                                  Long.box(Value.bigDecimal(bigDecimalPayload).value)
                                )
                            ),
                            FilterApi
                              .eq(FilterApi.binaryColumn("bigInteger"), Value.bigInteger(bigIntegerPayload).value)
                          ),
                          FilterApi
                            .eq(FilterApi.intColumn("dayOfWeek"), Int.box(Value.dayOfWeek(dayOfWeekPayload).value))
                        ),
                        FilterApi.eq(FilterApi.intColumn("month"), Int.box(Value.month(monthPayload).value))
                      ),
                      FilterApi.eq(FilterApi.binaryColumn("monthDay"), Value.monthDay(monthDayPayload).value)
                    ),
                    FilterApi.eq(FilterApi.binaryColumn("period"), Value.period(periodPayload).value)
                  ),
                  FilterApi.eq(FilterApi.intColumn("year"), Int.box(Value.year(yearPayload).value))
                ),
                FilterApi.eq(FilterApi.binaryColumn("yearMonth"), Value.yearMonth(yearMonthPayload).value)
              ),
              FilterApi.eq(FilterApi.binaryColumn("zoneId"), Value.zoneId(zoneIdPayload).value)
            ),
            FilterApi.eq(FilterApi.binaryColumn("zoneOffset"), Value.zoneOffset(zoneOffsetPayload).value)
          )

        val result2   = Filter.compile(
          duration === durationPayload or
            instant === instantPayload or
            localDate === localDatePayload or
            localTime === localTimePayload or
            localDateTime === localDateTimePayload or
            offsetTime === offsetTimePayload or
            offsetDateTime === offsetDateTimePayload or
            zonedDateTime === zonedDateTimePayload
        )
        val expected2 =
          FilterApi.or(
            FilterApi.or(
              FilterApi.or(
                FilterApi.or(
                  FilterApi.or(
                    FilterApi.or(
                      FilterApi.or(
                        FilterApi.eq(FilterApi.longColumn("duration"), Long.box(Value.duration(durationPayload).value)),
                        FilterApi.eq(FilterApi.longColumn("instant"), Long.box(Value.instant(instantPayload).value))
                      ),
                      FilterApi.eq(FilterApi.intColumn("localDate"), Int.box(Value.localDate(localDatePayload).value))
                    ),
                    FilterApi.eq(FilterApi.intColumn("localTime"), Int.box(Value.localTime(localTimePayload).value))
                  ),
                  FilterApi.eq(
                    FilterApi.longColumn("localDateTime"),
                    Long.box(Value.localDateTime(localDateTimePayload).value)
                  )
                ),
                FilterApi.eq(FilterApi.intColumn("offsetTime"), Int.box(Value.offsetTime(offsetTimePayload).value))
              ),
              FilterApi.eq(
                FilterApi.longColumn("offsetDateTime"),
                Long.box(Value.offsetDateTime(offsetDateTimePayload).value)
              )
            ),
            FilterApi.eq(
              FilterApi.longColumn("zonedDateTime"),
              Long.box(Value.zonedDateTime(zonedDateTimePayload).value)
            )
          )

        assert(result1)(isRight(equalTo(expected1))) &&
        assert(result2)(isRight(equalTo(expected2)))
      },
      test("compile option") {
        val (_, _, _, _, opt) = Filter.columns[MyRecord]

        val result   = Filter.compile(opt.nullable > 3)
        val expected = FilterApi.gt(FilterApi.intColumn("opt"), Int.box(Value.int(3).value))

        assert(result)(isRight(equalTo(expected)))
      },
      test("compile enum") {
        val (_, _, _, enm, _) = Filter.columns[MyRecord]

        val result   = Filter.compile(enm === MyRecord.Enum.Done)
        val expected = FilterApi.eq(FilterApi.binaryColumn("enm"), Value.string("Done").value)

        assert(result)(isRight(equalTo(expected)))
      },
      test("column path concatenation") {
        val (a, b, child, _, _) = Filter.columns[MyRecord]
        val (c, d)              = Filter.columns[MyRecord.Child]

        assert(a.path)(equalTo("a")) &&
        assert(b.path)(equalTo("b")) &&
        assert((child / c).path)(equalTo("child.c")) &&
        assert((child / d).path)(equalTo("child.d")) &&
        assert((d / child).path)(equalTo("d.child")) // TODO: must fail
      }
    )

}
