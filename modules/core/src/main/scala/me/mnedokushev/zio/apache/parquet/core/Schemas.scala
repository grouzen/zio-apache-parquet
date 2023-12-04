package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema._
import zio.Chunk

object Schemas {

  case class PrimitiveDef(
    typeName: PrimitiveTypeName,
    annotation: LogicalTypeAnnotation,
    isOptional: Boolean = false,
    length: Int = 0
  ) {

    def named(name: String): Type =
      Types
        .primitive(typeName, repetition(isOptional))
        .as(annotation)
        .length(length)
        .named(name)

    def optionality(condition: Boolean): PrimitiveDef =
      if (condition) optional else required

    def length(len: Int): PrimitiveDef =
      this.copy(length = len)

    def required: PrimitiveDef =
      this.copy(isOptional = true)

    def optional: PrimitiveDef =
      this.copy(isOptional = false)

  }

  case class RecordDef(fields: Chunk[Type], isOptional: Boolean = false) {

    def named(name: String): Type = {
      val builder = Types.buildGroup(repetition(isOptional))

      fields.foreach(builder.addField)
      builder.named(name)
    }

    def optionality(condition: Boolean): RecordDef =
      if (condition) optional else required

    def required: RecordDef =
      this.copy(isOptional = true)

    def optional: RecordDef =
      this.copy(isOptional = false)

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

}
