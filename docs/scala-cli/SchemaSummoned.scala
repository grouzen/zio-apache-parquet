//> using scala "3.5.0"
//> using dep me.mnedokushev::zio-apache-parquet-core:0.1.11

import me.mnedokushev.zio.apache.parquet.core.Schemas
import zio.schema.*
import me.mnedokushev.zio.apache.parquet.core.codec.*

object SchemaSummoned extends App:

  case class MyRecord(a: Int, b: String, c: Option[Long])

  object MyRecord:
    given schema: Schema[MyRecord] =
      DeriveSchema.gen[MyRecord]
    // The custom encoder must be defined before the definition for your record type.
    given SchemaEncoder[Int] with {
      override def encode(schema: Schema[Int], name: String, optional: Boolean) =
        Schemas.uuid.optionality(optional).named(name)
    }
    given schemaEncoder: SchemaEncoder[MyRecord] =
      Derive.derive[SchemaEncoder, MyRecord](SchemaEncoderDeriver.summoned)

  val parquetSchema = MyRecord.schemaEncoder.encode(MyRecord.schema, "my_record", optional = false)

  println(parquetSchema)
  // Outputs:
  // required group my_record {
  //   required fixed_len_byte_array(16) a (UUID);
  //   required binary b (STRING);
  //   optional int64 c (INTEGER(64,true));
  // }
