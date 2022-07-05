package io.sparkcyclone.native.transpiler

import scala.reflect.runtime.universe.reify
import scala.reflect.runtime.{universe, currentMirror => cm}
import scala.tools.reflect.ToolBox

/**
 * The toolbox is somewhat stateful. Depending on the very first expression it
 * typechecks, some fundamental types (e.g. Int, Long, etc.) will either be
 * comparable with =:= or not.
 *
 * For this reason we make one single primed toolbox available for use
 * everywhere.
 */
object CompilerToolBox {
  private val toolbox = cm.mkToolBox()
  toolbox.typecheck(reify { (a: Long) => a }.tree)

  def get: ToolBox[universe.type] = toolbox
}
