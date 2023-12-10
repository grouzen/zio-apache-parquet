package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import zio.Chunk

object Schemas {

  abstract class Def[Self <: Def[_]] {

    def named(name: String): Type

    def optionality(condition: Boolean): Self =
      if (condition) optional else required

    def required: Self

    def optional: Self

  }

  case class PrimitiveDef(
    typeName: PrimitiveTypeName,
    annotation: LogicalTypeAnnotation,
    isOptional: Boolean = false,
    length: Int = 0
  ) extends Def[PrimitiveDef] {

    def named(name: String): Type =
      Types
        .primitive(typeName, repetition(isOptional))
        .as(annotation)
        .length(length)
        .named(name)

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

  import PrimitiveTypeName._
  import LogicalTypeAnnotation._

  val string: PrimitiveDef  = PrimitiveDef(BINARY, stringType())
  val boolean: PrimitiveDef = PrimitiveDef(INT32, intType(8, false))
  val byte: PrimitiveDef    = PrimitiveDef(INT32, intType(8, false))
  val short: PrimitiveDef   = PrimitiveDef(INT32, intType(16, true))
  val int: PrimitiveDef     = PrimitiveDef(INT32, intType(32, true))
  val long: PrimitiveDef    = PrimitiveDef(INT64, intType(64, true))
  val uuid: PrimitiveDef    = PrimitiveDef(FIXED_LEN_BYTE_ARRAY, uuidType()).length(16)

  def record(fields: Chunk[Type]): RecordDef = RecordDef(fields)
  def list(element: Type): ListDef           = ListDef(element)
  def map(key: Type, value: Type)            = MapDef(key, value)

}
