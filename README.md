# aurora4spark

Spark Plugin development documentation: [aurora4spark-parent/README.md](aurora4spark-parent/README.md).

## Usage of the plugin

### on Aurora 5 or Aurora 6

```

$ /opt/spark/bin/spark-submit \
    --name PairwiseAddExample \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-add-pairwise.py

$ /opt/spark/bin/spark-submit \
    --name AveragingExample \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg.py

$ /opt/spark/bin/spark-submit \
    --name SumExample \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum.py

$ /opt/spark/bin/spark-submit \
    --name SumMultipleColumnsExample \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum-multiple.py


$ /opt/spark/bin/spark-submit \
    --name AveragingMultipleColumns5Example \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg-multiple.py

$ /opt/spark/bin/spark-submit \
    --name MultipleOperationsExample \
    --master 'local[4]' \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-multiple-operations.py

```

## NCC arguments

A good set of NCC defaults is set up, however if further overriding is needed, it can be done with the following Spark config:

```
--conf spark.com.nec.spark.ncc.debug=true
--conf spark.com.nec.spark.ncc.o=3
--conf spark.com.nec.spark.ncc.openmp=false
--conf spark.com.nec.spark.ncc.extra-argument.0=-X
--conf spark.com.nec.spark.ncc.extra-argument.1=-Y
```

For safety, if an argument key is not recognized, it will fail to launch.
