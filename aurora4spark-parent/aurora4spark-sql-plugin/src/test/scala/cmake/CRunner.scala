package cmake

import java.nio.file.Path
import com.sun.jna.Library

object CRunner {
  def runC(libPath: Path, functionName: String, args: Array[java.lang.Object]): Int = {
    // will abstract this out later
    import scala.collection.JavaConverters._
    val thingy2 =
      new Library.Handler(libPath.toString, classOf[Library], Map.empty[String, Any].asJava)
    val nl = thingy2.getNativeLibrary
    val fn = nl.getFunction(functionName)
    fn.invokeInt(args)
  }
}
