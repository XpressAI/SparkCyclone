package com.nec

import com.nec.aurora.Aurora
import com.nec.native.TransferDefinitions

import java.nio.file.Path
import java.nio.file.Paths

object VeDirectApp {

  def compile_c(): Path = {
    VeCompiler(compilationPrefix = "_spark", buildDir = Paths.get("_ve_build").toAbsolutePath)
      .compile_c {
        List(
          TransferDefinitions.TransferDefinitionsSourceCode,
          WordCount.WordCountSourceCode,
          Avg.AvgSourceCode,
          Sum.SumSourceCode,
          SumSimple.C_Definition,
          SumPairwise.C_Definition,
          AvgSimple.C_Definition,
          SumMultipleColumns.C_Definition,
          AvgMultipleColumns.C_Definition
        ).mkString("\n\n\n")
      }
  }

  def main(args: Array[String]): Unit = {
    val ve_so_name = compile_c().toString
    println(s"SO name: ${ve_so_name}")
    val proc = Aurora.veo_proc_create(0)
    println(s"Created proc = ${proc}")
    try {
      val ctx: Aurora.veo_thr_ctxt = Aurora.veo_context_open(proc)
      println(s"Created ctx = ${ctx}")
      try {
        val lib: Long = Aurora.veo_load_library(proc, ve_so_name)
        println(s"Lib = ${lib}")
        val vej = new VeJavaContext(ctx, lib)
        println(SumSimple.sum_doubles(vej, List(1, 2, 3, 4)))
        println(AvgSimple.avg_doubles(vej, List(1, 2, 3, 10)))
        val multiColumnSumResult = SumMultipleColumns.sum_multiple_doubles(
          vej,
          List(List(1, 2, 3), List(2, 3, 4), List(5, 4, 3), List(10, 10, 10))
        )
        println(multiColumnSumResult)
        assert(multiColumnSumResult == List(6.0, 9.0, 12.0, 30.0))
        val multiColumnAvgResult = AvgMultipleColumns.avg_multiple_doubles(
          vej,
          List(List(5, 10, 15), List(3, 27, 30), List(100, 200, 300), List(1000, 2000, 3000))
        )
        println(multiColumnAvgResult)
        assert(multiColumnAvgResult == List(10.0, 20.0, 200.0, 2000.0))
        println(
          SumPairwise.pairwise_sum_doubles(vej, List[(Double, Double)]((1, 1), (1, 2), (2, 9)))
        )

      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
  }

}
