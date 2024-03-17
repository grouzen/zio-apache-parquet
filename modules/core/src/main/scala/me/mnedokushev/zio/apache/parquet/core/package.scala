package me.mnedokushev.zio.apache.parquet

package object core {

  val MILLIS_PER_DAY    = 86400000L
  val NANOS_PER_DAY     = 86400000000000L
  val MILLIS_FACTOR     = 1000L
  val MICROS_FACTOR     = 1000000L
  val NANOS_FACTOR      = 1000000000L
  val DECIMAL_PRECISION = 11
  val DECIMAL_SCALE     = 2

  type Lens[F, S, A]   = filter.Column.Named[A, F]
  type Prism[F, S, A]  = Unit
  type Traversal[S, A] = Unit

}
