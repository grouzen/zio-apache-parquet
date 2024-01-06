package me.mnedokushev.zio.apache.parquet.core.filter

import me.mnedokushev.zio.apache.parquet.core.Value
import org.apache.parquet.filter2.predicate.FilterApi
import zio._
import zio.schema._
import zio.test._

import scala.jdk.CollectionConverters._

object ExprSpec extends ZIOSpecDefault {

  import Filter._

  case class MyRecord(a: String, b: Int)
  object MyRecord {
    // TODO: figure out how to avoid specifying the type explicitly in scala 2.13 and 3
    type A = "a"
    type B = "b"
    implicit val recordSchema: Schema.CaseClass2.WithFields[A, B, String, Int, MyRecord] =
      DeriveSchema.gen[MyRecord]
    // TODO: scala 2.12 works well without type annotation
    // implicit val recordSchema = DeriveSchema.gen[MyRecord]

    implicit val typeTag: TypeTag[MyRecord] = Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    val (a0, b0) = Filter.columns[MyRecord]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ExprSpec")(
      test("foo") {
        val result   = Expr.compile(not(MyRecord.a0 === "bar" && MyRecord.b0 > 1 && (MyRecord.b0 notIn Set(1, 2, 3))))
        val expected =
          FilterApi.not(
            FilterApi.and(
              FilterApi.and(
                FilterApi.eq(FilterApi.binaryColumn("a"), Value.string("bar").value),
                FilterApi.gt(FilterApi.intColumn("b"), Int.box(Value.int(1).value))
              ),
              FilterApi.notIn(FilterApi.intColumn("b"), Set(1, 2, 3).map(i => Int.box(Value.int(i).value)).asJava)
            )
          )

        assertTrue(result.contains(expected))
      }
    )

}
