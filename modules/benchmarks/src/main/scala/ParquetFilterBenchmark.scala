package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.{ ParquetWriter as P4sWriter, Path as P4sPath }
import me.mnedokushev.zio.apache.parquet.core.filter.{ Column, CompiledPredicate }
import me.mnedokushev.zio.apache.parquet.core.filter.syntax.*
import me.mnedokushev.zio.apache.parquet.hadoop.{ ParquetReader, Path }
import org.apache.parquet.column.statistics.IntStatistics
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.openjdk.jmh.annotations.*
import zio.Chunk

import scala.jdk.CollectionConverters.*
import scala.util.Using

import ParquetBenchmarkSupport.*
import ParquetWideRecords.*

object ParquetFilterBenchmark {

  @State(Scope.Thread)
  class FilterDataset extends TrialInput {
    @Param(Array("clustered", "scattered"))
    var layout: String = scala.compiletime.uninitialized

    var reader: ParquetReader[WideFlat]   = scala.compiletime.uninitialized
    var predicate: CompiledPredicate      = scala.compiletime.uninitialized
    private var expected: Chunk[WideFlat] = scala.compiletime.uninitialized
    private var rowGroups                 = 0
    private var candidateGroups           = 0

    @Setup(Level.Trial)
    def setup(): Unit = {
      createInput()
      val all               = flatRecords(rows)
      val (matching, other) = all.partition(matches)
      val clustered         = layout match {
        case "clustered" => true
        case "scattered" => false
        case unknown     => throw new IllegalArgumentException(s"Unknown filter layout: ${unknown}")
      }
      val ordered           = if (clustered) matching ++ other else all
      expected = matching
      require(expected.size == rows / 100 && expected.nonEmpty, s"Unexpected match count: ${expected.size}")

      val bucket        = Column.Named[Int, "bucket"]("bucket")
      predicate = filter(bucket === matchingBucket)
      require(predicate.isRight, s"Could not compile benchmark predicate: ${predicate}")
      val configuration = localConfiguration()
      reader = configuredReader[WideFlat](configuration)
      P4sWriter.of[WideFlat].options(writerOptions(configuration)).writeAndClose(P4sPath(input), ordered)

      val file           = HadoopInputFile.fromPath(Path(input).toHadoop, configuration)
      Using.resource(ParquetFileReader.open(file)) { parquet =>
        val blocks = parquet.getRowGroups.asScala
        rowGroups = blocks.size
        candidateGroups = blocks.count { block =>
          val column = block.getColumns.asScala.find(_.getPath.toDotString == bucket.path).get
          val stats  = column.getStatistics.asInstanceOf[IntStatistics]
          require(stats.hasNonNullValue, "Every row group must have bucket statistics")
          stats.getMin <= matchingBucket && stats.getMax >= matchingBucket
        }
      }
      require(rowGroups > 1, s"Expected multiple row groups, found ${rowGroups}")
      val matchFraction  = candidateGroups.toDouble / rowGroups
      val intendedLayout = if (clustered) matchFraction <= 0.25 else matchFraction >= 0.90
      require(
        candidateGroups > 0 && intendedLayout,
        s"${layout} has ${candidateGroups}/${rowGroups} candidate row groups"
      )
    }

    def verify(benchmark: ParquetFilterBenchmark): Unit = {
      require(benchmark.zioPushdownRead(this) == expected, "Pushdown returned different records")
      require(benchmark.zioInMemoryFilterRead(this) == expected, "In-memory filtering returned different records")
      println(
        s"${layout}, ${rows} rows: ${expected.size} matches, ${candidateGroups}/${rowGroups} candidate row groups"
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmark = new ParquetFilterBenchmark
    for {
      rows   <- List(10000, 524288)
      layout <- List("clustered", "scattered")
    } {
      val dataset = new FilterDataset
      dataset.rows = rows
      dataset.layout = layout
      try {
        dataset.setup()
        dataset.verify(benchmark)
      } finally dataset.cleanup()
    }
  }
}

class ParquetFilterBenchmark extends ParquetBenchmark {
  import ParquetFilterBenchmark.FilterDataset

  @Benchmark
  def zioPushdownRead(data: FilterDataset): Chunk[WideFlat] =
    run(data.reader.readChunkFiltered[WideFlat](Path(data.input), data.predicate))

  @Benchmark
  def zioInMemoryFilterRead(data: FilterDataset): Chunk[WideFlat] =
    run(data.reader.readChunk[WideFlat](Path(data.input)).map(_.filter(matches)))
}
