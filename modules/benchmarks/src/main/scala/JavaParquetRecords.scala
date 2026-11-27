package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.ParquetWriter.Options
import me.mnedokushev.zio.apache.parquet.hadoop.Path
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.{ ParquetFileReader, ParquetReader }
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.{ ExampleParquetWriter, GroupReadSupport }
import org.apache.parquet.hadoop.util.{ HadoopInputFile, HadoopOutputFile }
import org.apache.parquet.schema.MessageType
import zio.Chunk

import java.nio.file.Path as NioPath
import scala.util.Using

import ParquetBenchmarkSupport.{ Embedded, Record }
import ParquetWideRecords.*

object JavaParquetRecords {

  trait Mapping[A] {
    def decode(group: Group): A
    def encode(row: A, group: Group): Unit
  }

  def schema(file: NioPath, conf: Configuration): MessageType =
    Using.resource(ParquetFileReader.open(HadoopInputFile.fromPath(Path(file).toHadoop, conf))) { reader =>
      reader.getFileMetaData.getSchema
    }

  private class GroupReader(file: HadoopInputFile) extends ParquetReader.Builder[Group](file) {
    override protected def getReadSupport: ReadSupport[Group] = new GroupReadSupport()
  }

  def read[A: Mapping](file: NioPath, conf: Configuration): Chunk[A] = {
    val mapping = summon[Mapping[A]]
    val reader  = new GroupReader(HadoopInputFile.fromPath(Path(file).toHadoop, conf))
      .withConf(conf)
      .build()
    Using.resource(reader) { reader =>
      val records = Chunk.newBuilder[A]
      var group   = reader.read()
      while (group != null) {
        records += mapping.decode(group)
        group = reader.read()
      }
      records.result()
    }
  }

  def write[A: Mapping](file: NioPath, records: Chunk[A], schema: MessageType, options: Options): Unit = {
    val mapping = summon[Mapping[A]]
    val factory = new SimpleGroupFactory(schema)
    val writer  = ExampleParquetWriter
      .builder(HadoopOutputFile.fromPath(Path(file).toHadoop, options.hadoopConf))
      .withType(schema)
      .withWriteMode(options.writeMode)
      .withCompressionCodec(options.compressionCodecName)
      .withDictionaryEncoding(options.dictionaryEncodingEnabled)
      .withDictionaryPageSize(options.dictionaryPageSize)
      .withMaxPaddingSize(options.maxPaddingSize)
      .withPageSize(options.pageSize)
      .withRowGroupSize(options.rowGroupSize)
      .withValidation(options.validationEnabled)
      .withConf(options.hadoopConf)
      .build()
    Using.resource(writer) { writer =>
      records.foreach { row =>
        val group = factory.newGroup()
        mapping.encode(row, group)
        writer.write(group)
      }
    }
  }

  private def optionalString(group: Group, name: String): Option[String] =
    Option.when(group.getFieldRepetitionCount(name) != 0)(group.getString(name, 0))

  given Mapping[Record] with {
    def decode(group: Group): Record =
      Record(
        group.getInteger("i", 0),
        group.getString("dict", 0),
        Option.when(group.getFieldRepetitionCount("embedded") != 0) {
          val embedded = group.getGroup("embedded", 0)
          Embedded(embedded.getDouble("fraction", 0), embedded.getString("text", 0))
        }
      )

    def encode(row: Record, group: Group): Unit = {
      group.add("i", row.i)
      group.add("dict", row.dict)
      row.embedded.foreach { row =>
        val embedded = group.addGroup("embedded")
        embedded.add("fraction", row.fraction)
        embedded.add("text", row.text)
      }
    }
  }

  given Mapping[WideFlat] with {
    def decode(group: Group): WideFlat =
      WideFlat(
        group.getInteger("id", 0),
        group.getInteger("bucket", 0),
        optionalString(group, "campaign"),
        group.getLong("eventTime", 0),
        group.getString("sessionId", 0),
        group.getString("action", 0),
        group.getDouble("durationMs", 0),
        group.getLong("bytesIn", 0),
        group.getLong("bytesOut", 0),
        group.getInteger("attempts", 0),
        group.getBoolean("cached", 0),
        group.getLong("accountId", 0),
        group.getString("region", 0),
        group.getString("plan", 0),
        group.getString("device", 0),
        group.getString("os", 0),
        group.getString("appVersion", 0),
        group.getString("traceId", 0)
      )

    def encode(row: WideFlat, group: Group): Unit = {
      group.add("id", row.id)
      group.add("bucket", row.bucket)
      row.campaign.foreach(group.add("campaign", _))
      group.add("eventTime", row.eventTime)
      group.add("sessionId", row.sessionId)
      group.add("action", row.action)
      group.add("durationMs", row.durationMs)
      group.add("bytesIn", row.bytesIn)
      group.add("bytesOut", row.bytesOut)
      group.add("attempts", row.attempts)
      group.add("cached", row.cached)
      group.add("accountId", row.accountId)
      group.add("region", row.region)
      group.add("plan", row.plan)
      group.add("device", row.device)
      group.add("os", row.os)
      group.add("appVersion", row.appVersion)
      group.add("traceId", row.traceId)
    }
  }

  given Mapping[WideNested] with {
    def decode(group: Group): WideNested = {
      val event   = group.getGroup("event", 0)
      val metrics = event.getGroup("metrics", 0)
      val context = event.getGroup("context", 0)
      val client  = context.getGroup("client", 0)
      WideNested(
        group.getInteger("id", 0),
        group.getInteger("bucket", 0),
        optionalString(group, "campaign"),
        Event(
          event.getLong("eventTime", 0),
          event.getString("sessionId", 0),
          event.getString("action", 0),
          event.getDouble("durationMs", 0),
          Metrics(
            metrics.getLong("bytesIn", 0),
            metrics.getLong("bytesOut", 0),
            metrics.getInteger("attempts", 0),
            metrics.getBoolean("cached", 0)
          ),
          Context(
            context.getLong("accountId", 0),
            context.getString("region", 0),
            context.getString("plan", 0),
            Client(client.getString("device", 0), client.getString("os", 0), client.getString("appVersion", 0)),
            context.getString("traceId", 0)
          )
        )
      )
    }

    def encode(row: WideNested, group: Group): Unit = {
      group.add("id", row.id)
      group.add("bucket", row.bucket)
      row.campaign.foreach(group.add("campaign", _))
      val event = group.addGroup("event")
      event.add("eventTime", row.event.eventTime)
      event.add("sessionId", row.event.sessionId)
      event.add("action", row.event.action)
      event.add("durationMs", row.event.durationMs)

      val metrics = event.addGroup("metrics")
      metrics.add("bytesIn", row.event.metrics.bytesIn)
      metrics.add("bytesOut", row.event.metrics.bytesOut)
      metrics.add("attempts", row.event.metrics.attempts)
      metrics.add("cached", row.event.metrics.cached)

      val context = event.addGroup("context")
      context.add("accountId", row.event.context.accountId)
      context.add("region", row.event.context.region)
      context.add("plan", row.event.context.plan)
      val client  = context.addGroup("client")
      client.add("device", row.event.context.client.device)
      client.add("os", row.event.context.client.os)
      client.add("appVersion", row.event.context.client.appVersion)
      context.add("traceId", row.event.context.traceId)
    }
  }
}
