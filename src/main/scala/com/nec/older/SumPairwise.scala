package com.nec.older
import com.nec.LocationPointer
import com.nec.ve.VeJavaContext
import com.nec.aurora.Aurora
import org.bytedeco.javacpp.LongPointer
import org.bytedeco.javacpp.DoublePointer

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

  /** Leaky - todo deallocate */
  def pairwise_sum_doubles(
    veJavaContext: VeJavaContext,
    doubles: List[(Double, Double)]
  ): List[Double] = {
    val dataDoublePointerA = new DoublePointer(doubles.map(_._1): _*)
    val dataDoublePointerB = new DoublePointer(doubles.map(_._2): _*)
    val dataDoublePointerOut = new DoublePointer(doubles.map(_ => 0: Double): _*)
    pairwise_sum_doubles_mem(
      veJavaContext,
      dataDoublePointerA.address(),
      dataDoublePointerB.address(),
      dataDoublePointerOut.address(),
      doubles.length
    )
    doubles.indices.map(i => dataDoublePointerOut.get(i)).toList
  }

  def pairwise_sum_doubles_mem(
    veJavaContext: VeJavaContext,
    memLocationA: Long,
    memLocationB: Long,
    memLocationOut: Long,
    length: Int
  ): Unit = {
    import veJavaContext._
    val our_args = Aurora.veo_args_alloc()

    val a_ptr = new LocationPointer(memLocationA, length).asByteBuffer()
    val b_ptr = new LocationPointer(memLocationB, length).asByteBuffer()
    val out_ptr = new LocationPointer(memLocationOut, length).asByteBuffer()

    /** Put in the raw data */
    Aurora.veo_args_set_stack(our_args, 0, 0, a_ptr, 8 * length)
    Aurora.veo_args_set_stack(our_args, 0, 1, b_ptr, 8 * length)
    Aurora.veo_args_set_stack(our_args, 1, 2, out_ptr, 8 * length)
    Aurora.veo_args_set_i64(our_args, 3, length)

    /** Call */
    try {
      val req_id = Aurora.veo_call_async_by_name(ctx, lib, "sum_pairwise", our_args)
      val longPointer = new LongPointer(8)
      Aurora.veo_call_wait_result(ctx, req_id, longPointer)
    } finally our_args.close()
  }
}
