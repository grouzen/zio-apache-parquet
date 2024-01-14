package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import zio._
import zio.test.Assertion._
import zio.test._

import scala.jdk.CollectionConverters._

object ExprSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ExprSpec")(
      test("foo") {
        val (a, b) = Filter.columns[MyRecord]

        val result = Expr.compile(
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
      }
    )

}
