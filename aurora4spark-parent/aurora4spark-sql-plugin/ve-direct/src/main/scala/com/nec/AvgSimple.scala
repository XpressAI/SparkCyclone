package com.nec

import com.nec.VeFunction.StackArgument
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.{DoublePointer, LongPointer, Pointer}
import sun.nio.ch.DirectBuffer

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

  def avg_doubles(veJavaContext: VeJavaContext, doubles: List[Double]): Double = {

    /** Put in the raw data */
    val dataDoublePointer = new DoublePointer(doubles: _*)

    try avg_doubles_mem(
      veJavaContext,
      dataDoublePointer.asByteBuffer().asInstanceOf[DirectBuffer].address(),
      doubles.length
    )
    finally dataDoublePointer.close()
  }

  class LocationPointer(_addr: Long, _count: Long) extends Pointer {
    this.address = _addr
    this.limit = _count
    this.capacity = _count
  }
  def avg_doubles_mem(veJavaContext: VeJavaContext, memoryLocation: Long, count: Int): Double = {
    val our_args = Aurora.veo_args_alloc()

    import veJavaContext._

    Aurora.veo_args_set_stack(
      our_args,
      0,
      0,
      new LocationPointer(memoryLocation, count).asByteBuffer(),
      8 * count
    )
    Aurora.veo_args_set_i64(our_args, 1, count)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "avg", our_args)
      val longPointer = new LongPointer(8)
      Aurora.veo_call_wait_result(ctx, req_id, longPointer)
      longPointer.asByteBuffer().getDouble(0)
    } finally our_args.close()
  }
}
