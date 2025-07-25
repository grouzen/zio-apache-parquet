package me.mnedokushev.zio.apache.parquet.hadoop

import me.mnedokushev.zio.apache.parquet.core.codec.{SchemaEncoder, SchemaEncoderDeriver, ValueDecoder, ValueDecoderDeriver}
import me.mnedokushev.zio.apache.parquet.hadoop.ValueConverterSpec.FoodProductName.ProductName
import zio.schema.{Derive, DeriveSchema, Schema}
import zio.test.{Spec, TestEnvironment, ZIOSpecDefault, _}
import zio.{Scope, _}

object ValueConverterSpec extends ZIOSpecDefault {

  val dataPath =
    Path(getClass.getResource("/food.parquet").toURI)

  final case class FoodProductName(product_name: Option[List[Option[ProductName]]])

  object FoodProductName {

    final case class ProductName(
      lang: Option[String],
      text: Option[String]
    )

    implicit val schema: Schema[FoodProductName]               =
      DeriveSchema.gen[FoodProductName]
    implicit val schemaEncoder: SchemaEncoder[FoodProductName] =
      Derive.derive[SchemaEncoder, FoodProductName](SchemaEncoderDeriver.default)
    implicit val valueDecoder: ValueDecoder[FoodProductName]   =
      Derive.derive[ValueDecoder, FoodProductName](ValueDecoderDeriver.default)
  }

  final case class FoodBrandTags(brands_tags: Option[List[Option[String]]])

  object FoodBrandTags {
    implicit val schema: Schema[FoodBrandTags]               =
      DeriveSchema.gen[FoodBrandTags]
    implicit val schemaEncoder: SchemaEncoder[FoodBrandTags] =
      Derive.derive[SchemaEncoder, FoodBrandTags](SchemaEncoderDeriver.default)
    implicit val valueDecoder: ValueDecoder[FoodBrandTags]   =
      Derive.derive[ValueDecoder, FoodBrandTags](ValueDecoderDeriver.default)
  }

  // TODO: add more test cases
  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("ValueConvertedSpec")(
      test("read list of records") {
        for {
          reader <- ZIO.service[ParquetReader[FoodProductName]]
          result <- reader.readChunk(dataPath)
        } yield assertTrue(result.size == 10)
      }.provide(ParquetReader.projected[FoodProductName]()),
      test("read list of strings") {
        for {
          reader <- ZIO.service[ParquetReader[FoodBrandTags]]
          result <- reader.readChunk(dataPath)
        } yield assertTrue(result.size == 10)
      }.provide(ParquetReader.projected[FoodBrandTags]())
    )

}
