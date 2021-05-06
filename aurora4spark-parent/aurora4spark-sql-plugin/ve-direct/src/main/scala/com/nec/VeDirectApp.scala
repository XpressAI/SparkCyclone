package com.nec

import com.nec.aurora.Aurora

import java.nio.file.{Files, Paths}
import sun.misc.Unsafe

object VeDirectApp {

  private def getUnsafe: Unsafe = {
    val theUnsafe = classOf[Unsafe].getDeclaredField("theUnsafe")
    theUnsafe.setAccessible(true)
    theUnsafe.get(null).asInstanceOf[Unsafe]
  }

  def compile_c(): String = {
    val buildDir = Paths.get("_ve_build").toAbsolutePath
    if (!Files.exists(buildDir)) Files.createDirectory(buildDir)
    val cSource = buildDir.resolve("_sum.c")

    Files.write(
      cSource,
      s"""
     
      ${SumSimple.C_Definition} ${SumPairwise.C_Definition} ${AvgSimple.C_Definition}
       ${SumMultipleColumns.C_Definition} ${AvgMultipleColumns.C_Definition}
       """.getBytes("UTF-8")
    )
    val oFile = buildDir.resolve("_sum.o")
    val soFile = buildDir.resolve("_sum.so")
    import scala.sys.process._
    Seq(
      "ncc",
      "-O2",
      "-fpic",
      "-pthread",
      "-report-all",
      "-fdiag-vector=2",
      "-c",
      cSource.toString,
      "-o",
      oFile.toString
    ).!!
    Seq("ncc", "-shared", "-pthread", "-o", soFile.toString, oFile.toString).!!
    soFile.toString
  }

  def main(args: Array[String]): Unit = {
    val ve_so_name = compile_c()
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
        println(SumMultipleColumns.sum_multiple_doubles(vej, List(
          List(1,2,3),
          List(2,3,4),
          List(5,4,3)
        )))
        println(AvgMultipleColumns.avg_multiple_doubles(vej, List(
          List(5, 10, 15),
          List(3, 27, 30),
          List(100, 200, 300)
        )))
        println(
          SumPairwise.pairwise_sum_doubles(vej, List[(Double, Double)]((1, 1), (1, 2), (2, 9)))
        )

      } finally Aurora.veo_context_close(ctx)
    } finally Aurora.veo_proc_destroy(proc)
  }

}
