package org.apache.spark.sql.execution.joins;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

final class GeneratedIteratorForCodegenStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
    private Object[] references;
    private scala.collection.Iterator[] inputs;
    private scala.collection.Iterator localtablescan_input_0;
    private org.apache.spark.sql.execution.joins.UnsafeHashedRelation ourhashjoin_relation_0;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] ourhashjoin_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[3];

    public GeneratedIteratorForCodegenStage2(Object[] references) {
        this.references = references;
    }

    public void init(int index, scala.collection.Iterator[] inputs) {
        partitionIndex = index;
        this.inputs = inputs;
        localtablescan_input_0 = inputs[0];
        ourhashjoin_relation_0 = ((org.apache.spark.sql.execution.joins.UnsafeHashedRelation) ((org.apache.spark.broadcast.TorrentBroadcast) references[1] /* broadcast */).value()).asReadOnlyCopy();
        incPeakExecutionMemory(ourhashjoin_relation_0.estimatedSize());
        ourhashjoin_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
        ourhashjoin_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(3, 0);
        ourhashjoin_mutableStateArray_0[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(2, 0);
    }

    protected void processNext() throws java.io.IOException {
        // PRODUCE: Project [value#33, (mapTo#27 + 1.0) AS (mapTo + CAST(1 AS DOUBLE))#38]
        // PRODUCE: OurHashJoin [knownfloatingpointnormalized(normalizenanandzero((value#33 + 1.0)))], [knownfloatingpointnormalized(normalizenanandzero((num#26 + 1.0)))], Inner, BuildLeft, false
        // PRODUCE: LocalTableScan [num#26, mapTo#27]
        while (localtablescan_input_0.hasNext()) {
            InternalRow localtablescan_row_0 = (InternalRow) localtablescan_input_0.next();
            ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
            // CONSUME: OurHashJoin [knownfloatingpointnormalized(normalizenanandzero((value#33 + 1.0)))], [knownfloatingpointnormalized(normalizenanandzero((num#26 + 1.0)))], Inner, BuildLeft, false
            // input[0, double, false]
            double localtablescan_value_0 = localtablescan_row_0.getDouble(0);
            // generate join key for stream side
            ourhashjoin_mutableStateArray_0[0].reset();
            // knownfloatingpointnormalized(normalizenanandzero((input[0, double, false] + 1.0)))
            double ourhashjoin_value_2 = -1.0;
            ourhashjoin_value_2 = localtablescan_value_0 + 1.0D;
            double ourhashjoin_value_1 = -1.0;
            if (Double.isNaN(ourhashjoin_value_2)) {
                ourhashjoin_value_1 = Double.NaN;
            } else if (ourhashjoin_value_2 == -0.0d) {
                ourhashjoin_value_1 = 0.0d;
            } else {
                ourhashjoin_value_1 = ourhashjoin_value_2;
            }
            ourhashjoin_mutableStateArray_0[0].write(0, ourhashjoin_value_1);
            // find matches from HashRelation
            scala.collection.Iterator ourhashjoin_matches_0 = (ourhashjoin_mutableStateArray_0[0].getRow()).anyNull() ?
                    null : (scala.collection.Iterator) ourhashjoin_relation_0.get((ourhashjoin_mutableStateArray_0[0].getRow()));
            if (ourhashjoin_matches_0 != null) {
                while (ourhashjoin_matches_0.hasNext()) {
                    UnsafeRow ourhashjoin_matched_0 = (UnsafeRow) ourhashjoin_matches_0.next();
                    {
                        // CONSUME: Project [value#33, (mapTo#27 + 1.0) AS (mapTo + CAST(1 AS DOUBLE))#38]
                        // common sub-expressions
                        // CONSUME: WholeStageCodegen (2)
                        // input[0, double, true]
                        boolean ourhashjoin_isNull_5 = ourhashjoin_matched_0.isNullAt(0);
                        double ourhashjoin_value_5 = ourhashjoin_isNull_5 ?
                                -1.0 : (ourhashjoin_matched_0.getDouble(0));
                        // (input[2, double, false] + 1.0)
                        // input[1, double, false]
                        double localtablescan_value_1 = localtablescan_row_0.getDouble(1);
                        double project_value_1 = -1.0;
                        project_value_1 = localtablescan_value_1 + 1.0D;
                        ourhashjoin_mutableStateArray_0[2].reset();
                        ourhashjoin_mutableStateArray_0[2].zeroOutNullBytes();
                        if (ourhashjoin_isNull_5) {
                            ourhashjoin_mutableStateArray_0[2].setNullAt(0);
                        } else {
                            ourhashjoin_mutableStateArray_0[2].write(0, ourhashjoin_value_5);
                        }
                        ourhashjoin_mutableStateArray_0[2].write(1, project_value_1);
                        append((ourhashjoin_mutableStateArray_0[2].getRow()).copy());
                    }
                }
            }
            if (shouldStop()) return;
        }
    }
}