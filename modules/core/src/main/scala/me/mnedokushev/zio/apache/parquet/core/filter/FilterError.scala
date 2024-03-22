package me.mnedokushev.zio.apache.parquet.core.filter

final case class FilterError(
  message: String,
  cause: Option[Throwable] = None
) extends IllegalArgumentException(message, cause.getOrElse(new Throwable()))
