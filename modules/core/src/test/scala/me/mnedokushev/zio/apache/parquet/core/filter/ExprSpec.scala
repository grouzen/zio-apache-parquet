package me.mnedokushev.zio.apache.parquet.core.filter

import zio.test._
import zio._
import zio.schema._
// import org.apache.parquet.filter2.predicate.FilterApi
// import me.mnedokushev.zio.apache.parquet.core.Value

object ExprSpec extends ZIOSpecDefault {

  case class MyRecord(a: String, b: Int)
  object MyRecord {
    implicit val recordSchema: Schema.Record[MyRecord] = DeriveSchema.gen[MyRecord]
    implicit val typeTag: TypeTag[MyRecord]            = Derive.derive[TypeTag, MyRecord](TypeTagDeriver.default)

    val (a0, b0) = Filter.columns[MyRecord]
  }

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("ExprSpec")(
      test("foo") {
        // val result   = Expr.compile(MyRecord.a0 === "bar" and MyRecord.b0 > 1)
        // val expected =
        //   FilterApi.and(
        //     FilterApi.eq(FilterApi.binaryColumn("a"), Value.string("bar").value),
        //     FilterApi.gt(FilterApi.intColumn("b"), Int.box(Value.int(1).value))
        //   )

        // assertTrue(result.contains(expected))
        assertTrue(MyRecord.a0 != null)
      }
    )

}
