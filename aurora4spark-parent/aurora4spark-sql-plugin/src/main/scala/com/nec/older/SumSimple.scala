package com.nec.older
import com.nec.ve.VeJavaContext
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.DoublePointer
import com.nec.LocationPointer
import sun.nio.ch.DirectBuffer
import org.bytedeco.javacpp.LongPointer

object SumSimple {
  val C_Definition =
    """
      |double sum(double *a, int n)
      |{
      |    int i;
      |    double sum = 0;
      |    for (i = 0; i < n; i++) {
      |        sum += a[i];
      |    }
      |
      |    return sum;
      |}
      |""".stripMargin

  def sum_doubles(veJavaContext: VeJavaContext, doubles: List[Double]): Double = {

    val dataDoublePointer = new DoublePointer(doubles: _*)

    try sum_doubles_memory(
      veJavaContext,
      dataDoublePointer.asByteBuffer().asInstanceOf[DirectBuffer].address(),
      doubles.length
    )
    finally dataDoublePointer.close()
  }

  def sum_doubles_memory(
    veJavaContext: VeJavaContext,
    numbersMemoryAddress: Long,
    count: Int
  ): Double = {
    val our_args = Aurora.veo_args_alloc()

    import veJavaContext._

    Aurora.veo_args_set_stack(
      our_args,
      0,
      0,
      new LocationPointer(numbersMemoryAddress, count).asByteBuffer(),
      8 * count
    )
    Aurora.veo_args_set_i64(our_args, 1, count)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "sum", our_args)
      val longPointer = new LongPointer(8)
      try {
        Aurora.veo_call_wait_result(ctx, req_id, longPointer)
        longPointer.asByteBuffer().getDouble(0)
      } finally longPointer.close()
    } finally {
      Aurora.veo_args_free(our_args)
    }
  }
}
