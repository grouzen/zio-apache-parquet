import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

//val schema = Types
//  .list(Repetition.REQUIRED)
//  .requiredElement(PrimitiveTypeName.INT32)
//  .named("mylist")
//
//
//val groupSchema = schema.asGroupType()
//val listSchema = groupSchema.getFields.get(0).asGroupType()
//val listFieldName = listSchema.getName
//val elementName = listSchema.getFields.get(0).getName
//val listIndex = groupSchema.getFieldIndex(listFieldName)

val schema = Types
  .map(Repetition.REQUIRED)
  .key(PrimitiveTypeName.INT32)
  .requiredValue(PrimitiveTypeName.INT32)
  .named("mymap")

val groupSchema = schema.asGroupType()
val mapSchema = groupSchema.getFields.get(0).asGroupType()