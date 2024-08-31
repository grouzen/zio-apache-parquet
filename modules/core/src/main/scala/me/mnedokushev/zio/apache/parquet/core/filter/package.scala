package me.mnedokushev.zio.apache.parquet.core

import org.apache.parquet.filter2.predicate.FilterPredicate

package object filter {

  type CompiledPredicate = Either[String, FilterPredicate]

}
