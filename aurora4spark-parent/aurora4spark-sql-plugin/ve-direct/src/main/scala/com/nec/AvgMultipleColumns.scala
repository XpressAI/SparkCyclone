package com.nec

import com.nec.VeFunction.StackArgument
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.{DoublePointer, LongPointer}

object AvgMultipleColumns {
  val C_Definition = """
                       |void avg_multiple(double *a, double *b,int n, int m)
                       |{
                       |    int i;
                       |    int j;
                       |    for (i = 0; i < n; i++) {
                       |        double sum = 0;
                       |        for(j = 0; j< m; j++){
                       |           sum += a[(i*n) + j];
                       |        }
                       |        b[i] = sum/m;
                       |    }
                       |}
                       |""".stripMargin

  val Ve_F = VeFunction(
    name = "avg_multiple",
    args = List[StackArgument](
      StackArgument.ListOfDouble,
      StackArgument.ListOfDouble,
      StackArgument.Int,
      StackArgument.Int
    ),
    ret_type = None
  )

  def avg_multiple_doubles(veJavaContext: VeJavaContext,
                           inputs: List[List[Double]]): List[Double] = {
    import veJavaContext._
    val our_args = Aurora.veo_args_alloc()
    val inputFlattened = inputs.flatten

    val inputData = new DoublePointer(inputFlattened: _*)
    val dataDoublePointerOut = new DoublePointer(inputs.map(_ => 0: Double): _*)

    Aurora.veo_args_set_stack(our_args, 0, 0, inputData.asByteBuffer(), 8 * inputFlattened.size)
    Aurora.veo_args_set_stack(our_args, 1, 1, dataDoublePointerOut.asByteBuffer(), 8 * inputs.length)
    Aurora.veo_args_set_i64(our_args, 2, inputs.length)
    Aurora.veo_args_set_i64(our_args, 3, inputs.head.length)

    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "avg_multiple", our_args)
      val longPointer = new LongPointer(8)
      Aurora.veo_call_wait_result(ctx, req_id, longPointer)
      inputs.indices.map(i => dataDoublePointerOut.get(i)).toList
    } finally our_args.close()
  }
}
