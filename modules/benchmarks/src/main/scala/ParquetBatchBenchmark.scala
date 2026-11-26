package me.mnedokushev.zio.apache.parquet.benchmarks

import com.github.mjakubowski84.parquet4s.{ ParquetReader as P4sReader, ParquetWriter as P4sWriter, Path as P4sPath }
import me.mnedokushev.zio.apache.parquet.hadoop.Path
import org.openjdk.jmh.annotations.Benchmark
import zio.Chunk

import scala.util.Using

class ParquetBatchBenchmark extends ParquetBenchmark {
  import ParquetBenchmarkSupport.*

  @Benchmark
  def zioBatchRead(data: Dataset): Chunk[Record] =
    run(data.zioReader.readChunk[Record](Path(data.input)))

  @Benchmark
  def parquet4sBatchRead(data: Dataset): Chunk[Record] =
    Using.resource(P4sReader.as[Record].options(data.parquet4sReaderOptions).read(P4sPath(data.input))) { rows =>
      val builder = zio.ChunkBuilder.make[Record]()
      rows.iterator.foreach(row => builder += row)
      builder.result()
    }

  @Benchmark
  def zioBatchWrite(data: Dataset, output: Output): Unit =
    run(data.zioWriter.writeChunk(Path(output.file), data.chunk))

  @Benchmark
  def parquet4sBatchWrite(data: Dataset, output: Output): Unit =
    P4sWriter.of[Record].options(data.parquet4sOptions).writeAndClose(P4sPath(output.file), data.chunk)
}
