package me.mnedokushev.zio.apache.parquet.core.codec

import me.mnedokushev.zio.apache.parquet.core.Schemas
import me.mnedokushev.zio.apache.parquet.core.Schemas.PrimitiveDef
import zio._
import zio.schema._
import zio.test._

import java.util.UUID
//import scala.annotation.nowarn

object SchemaEncoderDeriverSpec extends ZIOSpecDefault {

  sealed trait MyEnum
  object MyEnum {
    case object Started    extends MyEnum
    case object InProgress extends MyEnum
    case object Done       extends MyEnum

    implicit val schema: Schema[MyEnum] = DeriveSchema.gen[MyEnum]
  }

  case class Record(a: Int, b: Option[String])
  object Record {
    implicit val schema: Schema[Record] = DeriveSchema.gen[Record]
  }

  case class BigRecord(
    a: Int, b: Option[String], c: Int, d:Int, e:Int, f:Int,
    g:Int, h:Int, i:Int, j:Int, k:Int, l:Int, m:Int, n:Int,
    o:Int, p:Int, q:Int, r:Int, s:Int, t:Int, u:Int, v:Int,
    w:Int
  )
  object BigRecord {
    implicit val schema: Schema[BigRecord] = DeriveSchema.gen[BigRecord]
  }

  case class MaxArityRecord(
    f1:Int, f2:Int, f3:Int, f4:Int, f5:Int, f6:Int, f7:Int, f8:Int, f9:Int, f10:Int,
    f11:Int, f12:Int, f13:Int, f14:Int, f15:Int, f16:Int, f17:Int, f18:Int, f19:Int, f20:Int,
    f21:Int, f22:Int, f23:Int, f24:Int, f25:Int, f26:Int, f27:Int, f28:Int, f29:Int, f30:Int,
    f31:Int, f32:Int, f33:Int, f34:Int, f35:Int, f36:Int, f37:Int, f38:Int, f39:Int, f40:Int,
    f41:Int, f42:Int, f43:Int, f44:Int, f45:Int, f46:Int, f47:Int, f48:Int, f49:Int, f50:Int,
    f51:Int, f52:Int, f53:Int, f54:Int, f55:Int, f56:Int, f57:Int, f58:Int, f59:Int, f60:Int,
    f61:Int, f62:Int, f63:Int, f64:Int, f65:Int, f66:Int, f67:Int, f68:Int, f69:Int, f70:Int,
    f71:Int, f72:Int, f73:Int, f74:Int, f75:Int, f76:Int, f77:Int, f78:Int, f79:Int, f80:Int,
    f81:Int, f82:Int, f83:Int, f84:Int, f85:Int, f86:Int, f87:Int, f88:Int, f89:Int, f90:Int,
    f91:Int, f92:Int, f93:Int, f94:Int, f95:Int, f96:Int, f97:Int, f98:Int, f99:Int, f100:Int,
    f101:Int, f102:Int, f103:Int, f104:Int, f105:Int, f106:Int, f107:Int, f108:Int, f109:Int, f110:Int,
    f111:Int, f112:Int, f113:Int, f114:Int, f115:Int, f116:Int, f117:Int, f118:Int, f119:Int, f120:Int,
    f121:Int, f122:Int, f123:Int, f124:Int, f125:Int, f126:Int, f127:Int, f128:Int, f129:Int, f130:Int,
    f131:Int, f132:Int, f133:Int, f134:Int, f135:Int, f136:Int, f137:Int, f138:Int, f139:Int, f140:Int,
    f141:Int, f142:Int, f143:Int, f144:Int, f145:Int, f146:Int, f147:Int, f148:Int, f149:Int, f150:Int,
    f151:Int, f152:Int, f153:Int, f154:Int, f155:Int, f156:Int, f157:Int, f158:Int, f159:Int, f160:Int,
    f161:Int, f162:Int, f163:Int, f164:Int, f165:Int, f166:Int, f167:Int, f168:Int, f169:Int, f170:Int,
    f171:Int, f172:Int, f173:Int, f174:Int, f175:Int, f176:Int, f177:Int, f178:Int, f179:Int, f180:Int,
    f181:Int, f182:Int, f183:Int, f184:Int, f185:Int, f186:Int, f187:Int, f188:Int, f189:Int, f190:Int,
    f191:Int, f192:Int, f193:Int, f194:Int, f195:Int, f196:Int, f197:Int, f198:Int, f199:Int, f200:Int,
    f201:Int, f202:Int, f203:Int, f204:Int, f205:Int, f206:Int, f207:Int, f208:Int, f209:Int, f210:Int,
    f211:Int, f212:Int, f213:Int, f214:Int, f215:Int, f216:Int, f217:Int, f218:Int, f219:Int, f220:Int,
    f221:Int, f222:Int, f223:Int, f224:Int, f225:Int, f226:Int, f227:Int, f228:Int, f229:Int, f230:Int,
    f231:Int, f232:Int, f233:Int, f234:Int, f235:Int, f236:Int, f237:Int, f238:Int, f239:Int, f240:Int,
    f241:Int, f242:Int, f243:Int, f244:Int, f245:Int, f246:Int, f247:Int, f248:Int, f249:Int, f250:Int,
    f251:Int, f252:Int, f253:Int, f254:Int
  )

  object MaxArityRecord {
    // unable to generate code for case classes with more than 120 int fields due to following error:
    // tested with jdk 11.0.23 64bit
    // Error while emitting me/mnedokushev/zio/apache/parquet/core/codec/SchemaEncoderDeriverSpec$MaxArityRecord$
    // Method too large: me/mnedokushev/zio/apache/parquet/core/codec/SchemaEncoderDeriverSpec$MaxArityRecord$.derivedSchema0$lzyINIT4$1$$anonfun$364 (Lscala/collection/immutable/ListMap;)Lscala/util/Either;
    // implicit val schema: Schema[MaxArityRecord] = DeriveSchema.gen[MaxArityRecord]
  }
  
  // Helper for being able to extract type parameter A from a given schema in order to cast the type of encoder<
  private def encode[A](encoder: SchemaEncoder[?], schema: Schema[A], name: String, optional: Boolean) =
    encoder.asInstanceOf[SchemaEncoder[A]].encode(schema, name, optional)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("SchemaEncoderDeriverSpec")(
      test("primitive") {
        def named(defs: List[PrimitiveDef], names: List[String]) =
          defs.zip(names).map { case (schemaDef, name) =>
            schemaDef.named(name)
          }

        val encoders: List[SchemaEncoder[?]] =
          List(
            Derive.derive[SchemaEncoder, String](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Boolean](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Byte](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Short](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Int](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, Long](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, UUID](SchemaEncoderDeriver.default)
          )
        val schemas: List[Schema[?]]         =
          List(
            Schema.primitive[String],
            Schema.primitive[Boolean],
            Schema.primitive[Byte],
            Schema.primitive[Short],
            Schema.primitive[Int],
            Schema.primitive[Long],
            Schema.primitive[UUID]
          )
        val names                            =
          List(
            "string",
            "boolean",
            "byte",
            "short",
            "int",
            "long",
            "uuid"
          )
        val schemaDefs                       = List(
          Schemas.string,
          Schemas.boolean,
          Schemas.byte,
          Schemas.short,
          Schemas.int,
          Schemas.long,
          Schemas.uuid
        )
        val optionalDefs                     =
          schemaDefs.map(_.optional)
        val requiredDefs                     =
          schemaDefs.map(_.required)

        val expectedOptional = named(optionalDefs, names)
        val expectedRequired = named(requiredDefs, names)

        encoders
          .zip(schemas)
          .zip(names)
          .zip(expectedOptional)
          .zip(expectedRequired)
          .map { case ((((encoder, schema), name), expOptional), expRequired) =>
            val tpeOptional = encode(encoder, schema, name, optional = true)
            val tpeRequired = encode(encoder, schema, name, optional = false)

            assertTrue(tpeOptional == expOptional, tpeRequired == expRequired)
          }
          .reduce(_ && _)
      },
      test("record") {
        val name        = "record"
        val encoder     = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.default)
        val tpeOptional = encoder.encode(Record.schema, name, optional = true)
        val tpeRequired = encoder.encode(Record.schema, name, optional = false)
        val schemaDef   = Schemas.record(
          Chunk(
            Schemas.int.required.named("a"),
            Schemas.string.optional.named("b")
          )
        )

        assertTrue(
          tpeOptional == schemaDef.optional.named(name),
          tpeRequired == schemaDef.required.named(name)
        )
      },
      test("arity > 22") {
        val name        = "arity"
        val encoder     = Derive.derive[SchemaEncoder, BigRecord](SchemaEncoderDeriver.default)
        val tpeOptional = encoder.encode(BigRecord.schema, name, optional = true)
        val tpeRequired = encoder.encode(BigRecord.schema, name, optional = false)
        val schemaDef   = Schemas.record(
          Chunk(
            Schemas.int.required.named("a"),
            Schemas.string.optional.named("b"),
            Schemas.int.required.named("c"),
            Schemas.int.required.named("d"),
            Schemas.int.required.named("e"),
            Schemas.int.required.named("f"),
            Schemas.int.required.named("g"),
            Schemas.int.required.named("h"),
            Schemas.int.required.named("i"),
            Schemas.int.required.named("j"),
            Schemas.int.required.named("k"),
            Schemas.int.required.named("l"),
            Schemas.int.required.named("m"),
            Schemas.int.required.named("n"),
            Schemas.int.required.named("o"),
            Schemas.int.required.named("p"),
            Schemas.int.required.named("q"),
            Schemas.int.required.named("r"),
            Schemas.int.required.named("s"),
            Schemas.int.required.named("t"),
            Schemas.int.required.named("u"),
            Schemas.int.required.named("v"),
            Schemas.int.required.named("w")
          )
        )

        assertTrue(
          tpeOptional == schemaDef.optional.named(name),
          tpeRequired == schemaDef.required.named(name)
        )
      },
      test("sequence") {
        val name                             = "mylist"
        val encoders: List[SchemaEncoder[?]] =
          List(
            Derive.derive[SchemaEncoder, List[String]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Boolean]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Byte]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Short]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Int]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Long]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[UUID]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[String]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Boolean]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Byte]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Short]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Int]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[Long]]](SchemaEncoderDeriver.default),
            Derive.derive[SchemaEncoder, List[Option[UUID]]](SchemaEncoderDeriver.default)
          )
        val schemas: List[Schema[?]]         =
          List(
            Schema.list[String],
            Schema.list[Int],
            Schema.list[Option[String]],
            Schema.list[Option[Int]]
          )
        val elements                         =
          List(
            Schemas.string,
            Schemas.boolean,
            Schemas.byte,
            Schemas.short,
            Schemas.int,
            Schemas.long,
            Schemas.uuid
          )
        val schemaDefs                       =
          (elements.map(_.required) ++ elements.map(_.optional))
            .map(_.named("element"))
            .map(Schemas.list)
        val expectedOptional                 =
          schemaDefs.map(_.optional.named(name))
        val expectedRequired                 =
          schemaDefs.map(_.required.named(name))

        encoders
          .zip(schemas)
          .zip(expectedOptional)
          .zip(expectedRequired)
          .map { case (((encoder, schema), expOptional), expRequired) =>
            val tpeOptional = encode(encoder, schema, name, optional = true)
            val tpeRequired = encode(encoder, schema, name, optional = false)

            assertTrue(
              tpeOptional == expOptional,
              tpeRequired == expRequired
            )
          }
          .reduce(_ && _)
      },
      test("map") {
        val name    = "mymap"
        val encoder = Derive.derive[SchemaEncoder, Map[String, Int]](SchemaEncoderDeriver.default)
        val tpe     = encoder.encode(Schema.map[String, Int], name, optional = true)

        assertTrue(
          tpe == Schemas
            .map(Schemas.string.required.named("key"), Schemas.int.required.named("value"))
            .optional
            .named(name)
        )
      },
      test("enum") {
        val name    = "myenum"
        val encoder = Derive.derive[SchemaEncoder, MyEnum](SchemaEncoderDeriver.default)
        val tpe     = encoder.encode(Schema[MyEnum], name, optional = true)

        assertTrue(tpe == Schemas.enum0.optional.named(name))
      }
//      test("summoned") {
      //        // @nowarn annotation is needed to avoid having 'variable is not used' compiler error
      //        @nowarn
      //        implicit val intEncoder: SchemaEncoder[Int] = new SchemaEncoder[Int] {
      //          override def encode(schema: Schema[Int], name: String, optional: Boolean): Type =
      //            Schemas.uuid.optionality(optional).named(name)
      //        }
      //
      //        val name    = "myrecord"
      //        val encoder = Derive.derive[SchemaEncoder, Record](SchemaEncoderDeriver.summoned)
      //        val tpe     = encoder.encode(Record.schema, name, optional = true)
      //
      //        assertTrue(
      //          tpe == Schemas
      //            .record(Chunk(Schemas.uuid.required.named("a"), Schemas.string.optional.named("b")))
      //            .optional
      //            .named(name)
      //        )
      //      }
    )

}
