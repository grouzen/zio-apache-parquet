//> using scala "3.7.4"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.3.2

import zio.schema._
import me.mnedokushev.zio.apache.parquet.core.codec._

object SchemaArity23 extends App {

  final case class Arity23(
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
    implicit val schema: Schema[Arity23]               =
      DeriveSchema.gen[Arity23]
    implicit val schemaEncoder: SchemaEncoder[Arity23] =
      Derive.derive[SchemaEncoder, Arity23](SchemaEncoderDeriver.default)
  }

  val arity23Schema = Arity23.schemaEncoder.encode(Arity23.schema, "arity23", optional = false)

  println(arity23Schema)

}
