package com.nec.ve

import com.eed3si9n.expecty.Expecty.expect
import com.nec.arrow.ArrowVectorBuilders.withArrowFloat8VectorI
import com.nec.arrow.WithTestAllocator
import com.nec.spark.agile.CExpressionEvaluation.CodeLines
import com.nec.spark.agile.CFunctionGeneration.VeScalarType
import com.nec.spark.agile.groupby.GroupByOutline
import com.nec.util.RichVectors.RichFloat8
import com.nec.ve.VeColBatch.VeColVector
import org.apache.arrow.vector.Float8Vector
import org.scalatest.freespec.AnyFreeSpec

final class ArrowTransferCheck extends AnyFreeSpec with WithVeProcess with VeKernelInfra {
  "Identify check: data that we put into the VE can be retrieved back out" - {
    "for Float8Vector" in {
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val arrowVec = colVec.toArrowVector()

          try {
            colVec.free()
            expect(arrowVec.toString == f8v.toString)
          } finally arrowVec.close()
        }
      }
    }
  }

  "Execute our function" in {
    compiledWithHeaders(
      CodeLines
        .from(
          s"long f(nullable_double_vector* input, nullable_double_vector* o) {",
          CodeLines
            .from(
              GroupByOutline.initializeScalarVector(VeScalarType.VeNullableDouble, "o", "i->count"),
              "for ( int i = 0; i < input->count; i++ ) {",
              CodeLines.from("o->data[i] = input->data[i] * 2;").indented,
              "}"
            )
            .indented,
          "}"
        )
        .cCode
    ) { path =>
      val lib = veProcess.loadLibrary(path)
      WithTestAllocator { implicit alloc =>
        withArrowFloat8VectorI(List(1, 2, 3)) { f8v =>
          val colVec: VeColVector = VeColVector.fromFloat8Vector(f8v)
          val results = veProcess.execute(
            libraryReference = lib,
            functionName = "f",
            cols = List(colVec),
            results = List(VeScalarType.veNullableDouble)
          )

          expect(results.size == 1)
          val result = results.head.toArrowVector().asInstanceOf[Float8Vector].toList
          expect(result == List[Double](2, 4, 6))
        }
      }
    }
  }
}
