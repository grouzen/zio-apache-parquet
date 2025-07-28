//> using scala "3.7.1"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.3.1

import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*

object Schema extends App:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    given schema: Schema[MyRecord]               =
      DeriveSchema.gen[MyRecord]
    given schemaEncoder: SchemaEncoder[MyRecord] =
      Derive.derive[SchemaEncoder, MyRecord](SchemaEncoderDeriver.default)

  val parquetSchema = MyRecord.schemaEncoder.encode(MyRecord.schema, "my_record", optional = false)

  println(parquetSchema)
  // Outputs:
  // required group my_record {
  //   required int32 a (INTEGER(32,true));
  //   required binary b (STRING);
  //   optional int64 c (INTEGER(64,true));
  // }
