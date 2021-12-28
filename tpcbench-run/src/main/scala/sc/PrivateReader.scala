package sc

import org.apache.commons.lang3.reflect.FieldUtils
import scala.language.dynamics

final class PrivateReader(val obj: Object) extends Dynamic {
  def selectDynamic(name: String): PrivateReader = {
    val clz = obj.getClass
    val field = FieldUtils.getAllFields(clz).find(_.getName == name) match {
      case Some(f) => f
      case None    => throw new NoSuchFieldException(s"Class $clz does not seem to have $name")
    }
    field.setAccessible(true)
    new PrivateReader(field.get(obj))
  }
}
object PrivateReader {
  implicit class RichObject(obj: Object) {
    def readPrivate: PrivateReader = new PrivateReader(obj)
  }
}
