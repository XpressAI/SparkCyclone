package com.nec

import com.nec.VeCallContext.{IntArgument, ListDoubleArgument}
import com.nec.VeFunction.StackArgument

object AvgSimple {
  val C_Definition =
    """
      |double avg(double *a, int n)
      |{
      |    int i;
      |    double sum = 0;
      |    for (i = 0; i < n; i++) {
      |        sum += a[i];
      |    }
      |
      |    return sum/i;
      |}
      |""".stripMargin

  val Ve_F = VeFunction(
    name = "avg",
    args = List[StackArgument](StackArgument.ListOfDouble, StackArgument.Int),
    ret_type = Some("'double'")
  )

  def avgSimple(veCallContext: VeCallContext, inputs: List[Double]): Double = {
    veCallContext.execute(
      veFunction = Ve_F,
      uploadData = { poss =>
        inputs.iterator.zipWithIndex.foreach { case (a, idx) =>
          poss.args(0).foreach { Pos_In_1 =>
            veCallContext.unsafe.putDouble(Pos_In_1 + idx * 8, a)
          }
        }
      },
      loadData = { (ret, poss) =>
        ret.as[Double]
      },
      arguments = Seq(ListDoubleArgument(inputs.size), IntArgument(inputs.size))
    )
  }
}
