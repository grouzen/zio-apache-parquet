package me.mnedokushev.zio.apache.parquet.benchmarks

import me.mnedokushev.zio.apache.parquet.core.codec.*
import zio.Chunk
import zio.schema.{ Derive, DeriveSchema, Schema }

import java.util.{ Random, UUID }

object ParquetWideRecords {

  case class WideFlat(
    id: Int,
    bucket: Int,
    campaign: Option[String],
    eventTime: Long,
    sessionId: String,
    action: String,
    durationMs: Double,
    bytesIn: Long,
    bytesOut: Long,
    attempts: Int,
    cached: Boolean,
    accountId: Long,
    region: String,
    plan: String,
    device: String,
    os: String,
    appVersion: String,
    traceId: String
  )

  object WideFlat {
    given Schema[WideFlat]        = DeriveSchema.gen[WideFlat]
    given SchemaEncoder[WideFlat] = Derive.derive[SchemaEncoder, WideFlat](SchemaEncoderDeriver.default)
    given ValueEncoder[WideFlat]  = Derive.derive[ValueEncoder, WideFlat](ValueEncoderDeriver.default)
    given ValueDecoder[WideFlat]  = Derive.derive[ValueDecoder, WideFlat](ValueDecoderDeriver.default)
  }

  case class WideNested(id: Int, bucket: Int, campaign: Option[String], event: Event)
  case class Event(
    eventTime: Long,
    sessionId: String,
    action: String,
    durationMs: Double,
    metrics: Metrics,
    context: Context
  )
  case class Metrics(bytesIn: Long, bytesOut: Long, attempts: Int, cached: Boolean)
  case class Context(
    accountId: Long,
    region: String,
    plan: String,
    client: Client,
    traceId: String
  )
  case class Client(device: String, os: String, appVersion: String)

  object WideNested {
    given Schema[WideNested]        = DeriveSchema.gen[WideNested]
    given SchemaEncoder[WideNested] = Derive.derive[SchemaEncoder, WideNested](SchemaEncoderDeriver.default)
    given ValueEncoder[WideNested]  = Derive.derive[ValueEncoder, WideNested](ValueEncoderDeriver.default)
    given ValueDecoder[WideNested]  = Derive.derive[ValueDecoder, WideNested](ValueDecoderDeriver.default)
  }

  val matchingBucket = 1

  def matches(record: WideFlat): Boolean =
    record.bucket == matchingBucket

  def flatRecords(rows: Int): Chunk[WideFlat] = {
    val random  = new Random(42L)
    val actions = Vector("open", "browse", "submit")
    val regions = Vector("na", "eu", "apac", "latam")
    val systems = Vector("linux", "android", "macos", "ios")
    Chunk.fromIterable(Vector.tabulate(rows) { index =>
      val id = index + 1
      WideFlat(
        id = id,
        bucket = if (id % 100 == 0) matchingBucket else 0,
        campaign = Option.when(id % 7 == 0)(s"campaign-${id % 17}"),
        eventTime = 1700000000000L + id * 1000L,
        sessionId = new UUID(random.nextLong(), random.nextLong()).toString,
        action = actions(id % actions.size),
        durationMs = random.nextDouble() * 2000.0,
        bytesIn = random.nextInt(16384).toLong,
        bytesOut = random.nextInt(262144).toLong,
        attempts = id % 4 + 1,
        cached = id   % 2 == 0,
        accountId = random.nextInt(50000).toLong,
        region = regions((id / 3) % regions.size),
        plan = if (id % 4 == 0) "paid" else "free",
        device = if (id % 2 == 0) "desktop" else "mobile",
        os = systems(id % systems.size),
        appVersion = if (id % 8 == 0) "2.0" else "2.1",
        traceId = new UUID(random.nextLong(), random.nextLong()).toString
      )
    })
  }

  def nestedRecords(rows: Int): Chunk[WideNested] =
    flatRecords(rows).map { row =>
      WideNested(
        row.id,
        row.bucket,
        row.campaign,
        Event(
          row.eventTime,
          row.sessionId,
          row.action,
          row.durationMs,
          Metrics(row.bytesIn, row.bytesOut, row.attempts, row.cached),
          Context(
            row.accountId,
            row.region,
            row.plan,
            Client(row.device, row.os, row.appVersion),
            row.traceId
          )
        )
      )
    }
}
