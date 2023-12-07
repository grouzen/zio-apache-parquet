package me.mnedokushev.zio.apache.parquet.core.hadoop

import org.apache.hadoop.fs.{ Path => HadoopPath }

import java.net.URI
import java.nio.file.{ Path => JPath, Paths }

case class Path(underlying: HadoopPath) {

  def /(child: String): Path =
    this.copy(underlying = new HadoopPath(underlying, child))

  def /(child: JPath): Path =
    this.copy(underlying = new HadoopPath(underlying, Path(child).underlying))

  def toJava: JPath =
    Paths.get(underlying.toUri)

}

object Path {

  def apply(path: JPath): Path =
    Path(new HadoopPath(new URI("file", null, path.toAbsolutePath.toString, null, null)))

}
