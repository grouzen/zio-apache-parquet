package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import zio.Chunk

object Schemas {

  abstract class Def[Self <: Def[?]] {

    def named(name: String): Type

    def optionality(condition: Boolean): Self =
      if (condition) optional else required

    def required: Self

    def optional: Self

  }

  case class PrimitiveDef(
    typeName: PrimitiveTypeName,
    annotation: Option[LogicalTypeAnnotation] = None,
    isOptional: Boolean = false,
    length: Int = 0
  ) extends Def[PrimitiveDef] {

    def named(name: String): Type = {
      val builder = Types.primitive(typeName, repetition(isOptional))

      annotation
        .fold(builder)(builder.as)
        .length(length)
        .named(name)
    }

    def length(len: Int): PrimitiveDef =
      this.copy(length = len)

    def required: PrimitiveDef =
      this.copy(isOptional = false)

    def optional: PrimitiveDef =
      this.copy(isOptional = true)

  }

  case class RecordDef(fields: Chunk[Type], isOptional: Boolean = false) extends Def[RecordDef] {

    def named(name: String): Type = {
      val builder = Types.buildGroup(repetition(isOptional))

      fields.foreach(builder.addField)
      builder.named(name)
    }

    def required: RecordDef =
      this.copy(isOptional = false)

    def optional: RecordDef =
      this.copy(isOptional = true)

  }

  case class ListDef(
    element: Type,
    isOptional: Boolean = false
  ) extends Def[ListDef] {

    def named(name: String): Type =
      Types
        .list(repetition(isOptional))
        .element(element)
        .named(name)

    def required: ListDef =
      this.copy(isOptional = false)

    def optional: ListDef =
      this.copy(isOptional = true)

  }

  case class MapDef(key: Type, value: Type, isOptional: Boolean = false) extends Def[MapDef] {

    override def named(name: String): Type =
      Types
        .map(repetition(isOptional))
        .key(key)
        .value(value)
        .named(name)

    override def required: MapDef =
      this.copy(isOptional = false)

    override def optional: MapDef =
      this.copy(isOptional = true)

  }

  def repetition(optional: Boolean): Repetition =
    if (optional) Repetition.OPTIONAL else Repetition.REQUIRED

  def asMessageType(schema: Type): MessageType = {
    val groupSchema = schema.asGroupType()
    val name        = groupSchema.getName
    val fields      = groupSchema.getFields

    new MessageType(name, fields)
  }

  import PrimitiveTypeName._
  import LogicalTypeAnnotation._

  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
  def enum0: PrimitiveDef          = PrimitiveDef(BINARY, Some(enumType()))
  val string: PrimitiveDef         = PrimitiveDef(BINARY, Some(stringType()))
  val boolean: PrimitiveDef        = PrimitiveDef(BOOLEAN)
  val byte: PrimitiveDef           = PrimitiveDef(INT32, Some(intType(8, false)))
  val short: PrimitiveDef          = PrimitiveDef(INT32, Some(intType(16, true)))
  val int: PrimitiveDef            = PrimitiveDef(INT32, Some(intType(32, true)))
  val long: PrimitiveDef           = PrimitiveDef(INT64, Some(intType(64, true)))
  val float: PrimitiveDef          = PrimitiveDef(FLOAT)
  val double: PrimitiveDef         = PrimitiveDef(DOUBLE)
  val binary: PrimitiveDef         = PrimitiveDef(BINARY)
  val char: PrimitiveDef           = byte
  val uuid: PrimitiveDef           = PrimitiveDef(FIXED_LEN_BYTE_ARRAY, Some(uuidType())).length(16)
  val bigDecimal: PrimitiveDef     = PrimitiveDef(INT64, Some(decimalType(DECIMAL_PRECISION, DECIMAL_SCALE)))
  val bigInteger: PrimitiveDef     = PrimitiveDef(BINARY)
  val dayOfWeek: PrimitiveDef      = byte
  val monthType: PrimitiveDef      = byte
  val monthDay: PrimitiveDef       = PrimitiveDef(FIXED_LEN_BYTE_ARRAY).length(2)
  val period: PrimitiveDef         = PrimitiveDef(FIXED_LEN_BYTE_ARRAY).length(12)
  val year: PrimitiveDef           = PrimitiveDef(INT32, Some(intType(16, false)))
  val yearMonth: PrimitiveDef      = PrimitiveDef(FIXED_LEN_BYTE_ARRAY).length(4)
  val zoneId: PrimitiveDef         = string
  val zoneOffset: PrimitiveDef     = string
  val duration: PrimitiveDef       = PrimitiveDef(INT64, Some(intType(64, false)))
  val instant: PrimitiveDef        = PrimitiveDef(INT64, Some(intType(64, false)))
  val localDate: PrimitiveDef      = PrimitiveDef(INT32, Some(dateType()))
  val localTime: PrimitiveDef      = PrimitiveDef(INT32, Some(timeType(true, TimeUnit.MILLIS)))
  val localDateTime: PrimitiveDef  = PrimitiveDef(INT64, Some(timestampType(true, TimeUnit.MILLIS)))
  val offsetTime: PrimitiveDef     = PrimitiveDef(INT32, Some(timeType(false, TimeUnit.MILLIS)))
  val offsetDateTime: PrimitiveDef = PrimitiveDef(INT64, Some(timestampType(false, TimeUnit.MILLIS)))
  val zonedDateTime: PrimitiveDef  = offsetDateTime

  def record(fields: Chunk[Type]): RecordDef = RecordDef(fields)
  def list(element: Type): ListDef           = ListDef(element)
  def map(key: Type, value: Type): MapDef    = MapDef(key, value)

}
