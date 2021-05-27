import SumCSpec.withArrowFloat8Vector
import com.nec.{Avg, Sum}
import org.scalatest.freespec.AnyFreeSpec

final class AvgSpec extends AnyFreeSpec {
  "JVM avg works" in {
    val inputData: Seq[Seq[Double]] = Seq(
      Seq(3.0, 19, 23.0),
      Seq(10.0, 20.0, 30.0),
      Seq(50.0 , 60.0, 70.0)
    )

    withArrowFloat8Vector(inputData) { vcv =>
      assert(Avg.avgJVM(vcv, 3) == Seq(21.0, 33.0, 41.0))
    }
  }
}
