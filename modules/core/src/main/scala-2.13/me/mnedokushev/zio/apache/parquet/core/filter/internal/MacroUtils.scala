package me.mnedokushev.zio.apache.parquet.core.filter.internal

import scala.reflect.macros.blackbox

abstract class MacroUtils(c: blackbox.Context) {

  import c.universe._

  private def debugEnabled: Boolean = true

  implicit class Debugged[A](self: A) {
    def debugged(): Unit =
      if (debugEnabled)
        c.info(c.enclosingPosition, s"tree=${showRaw(self)}", force = true)
  }

}
