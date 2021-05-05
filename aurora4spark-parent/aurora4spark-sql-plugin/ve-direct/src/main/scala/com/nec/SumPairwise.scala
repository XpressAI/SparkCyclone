package com.nec

import com.nec.VeFunction.StackArgument
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.{DoublePointer, LongPointer}

object SumPairwise {
  val C_Definition = """
                       |void sum_pairwise(double *a, double *b, double *c, int n)
                       |{
                       |    int i;
                       |    double sum = 0;
                       |    for (i = 0; i < n; i++) {
                       |        c[i] = a[i] + b[i];
                       |    }
                       |}
                       |""".stripMargin

  val Ve_F = VeFunction(
    name = "sum_pairwise",
    args = List[StackArgument](
      StackArgument.ListOfDouble,
      StackArgument.ListOfDouble,
      StackArgument.ListOfDouble,
      StackArgument.Int
    ),
    ret_type = None
  )

  /** Leaky - todo deallocate */
  def pairwise_sum_doubles(
    veJavaContext: VeJavaContext,
    doubles: List[(Double, Double)]
  ): List[Double] = {
    import veJavaContext._
    val our_args = Aurora.veo_args_alloc()

    /** Put in the raw data */
    val dataDoublePointerA = new DoublePointer(doubles.map(_._1): _*)
    val dataDoublePointerB = new DoublePointer(doubles.map(_._2): _*)
    val dataDoublePointerOut = new DoublePointer(doubles.map(_ => 0: Double): _*)
    Aurora.veo_args_set_stack(our_args, 0, 0, dataDoublePointerA.asByteBuffer(), 8 * doubles.length)
    Aurora.veo_args_set_stack(our_args, 0, 1, dataDoublePointerB.asByteBuffer(), 8 * doubles.length)
    Aurora.veo_args_set_stack(
      our_args,
      1,
      2,
      dataDoublePointerOut.asByteBuffer(),
      8 * doubles.length
    )
    Aurora.veo_args_set_i64(our_args, 3, doubles.length)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "sum_pairwise", our_args)
      val longPointer = new LongPointer(8)
      Aurora.veo_call_wait_result(ctx, req_id, longPointer)
      doubles.indices.map(i => dataDoublePointerOut.get(i)).toList
    } finally our_args.close()
  }
}
