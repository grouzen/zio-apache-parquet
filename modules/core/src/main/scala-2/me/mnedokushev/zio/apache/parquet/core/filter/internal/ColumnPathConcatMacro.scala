package me.mnedokushev.zio.apache.parquet.core.filter.internal

import scala.reflect.macros.blackbox
import me.mnedokushev.zio.apache.parquet.core.filter.Column
import me.mnedokushev.zio.apache.parquet.core.filter.{ TypeTag => TypeTag0 }

class ColumnPathConcatMacro(val c: blackbox.Context) extends MacroUtils(c) {
  import c.universe._

  def concatImpl[B](child: Expr[Column[B]])(typeTag: Expr[TypeTag0[B]]): Tree = {
    // c.abort(c.enclosingPosition, s"column ${child.tree}")
    // val b = child.toString()
    // c.info(c.enclosingPosition, s"Outcome: $b", force = true)
    
    c.prefix.debugged()
    child.debugged()
    c.prefix.asInstanceOf[Column[_]].typeTag.debugged()

    val _ = typeTag
    child.tree
  }

}
