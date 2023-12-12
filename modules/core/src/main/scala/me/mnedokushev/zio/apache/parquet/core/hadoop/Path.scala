package me.mnedokushev.zio.apache.parquet.core.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ Path => HadoopPath }
import org.apache.parquet.hadoop.util.HadoopInputFile
import zio._

import java.io.IOException
import java.net.URI
import java.nio.file.{ Path => JPath, Paths }

case class Path(underlying: HadoopPath) {

  def /(child: String): Path =
    this.copy(underlying = new HadoopPath(underlying, child))

  def /(child: JPath): Path =
    this.copy(underlying = new HadoopPath(underlying, Path(child).underlying))

  def toJava: JPath =
    Paths.get(underlying.toUri)

  def toHadoop: HadoopPath =
    underlying

  def toInputFileZIO(conf: Configuration): IO[IOException, HadoopInputFile] =
    ZIO.attemptBlockingIO(HadoopInputFile.fromPath(underlying, conf))

}

object Path {

  def apply(path: JPath): Path =
    Path(new HadoopPath(new URI("file", null, path.toAbsolutePath.toString, null, null)))

}
