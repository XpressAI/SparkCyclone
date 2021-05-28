# aurora4spark

Spark Plugin development documentation: [aurora4spark-parent/README.md](aurora4spark-parent/README.md).

## Usage of the plugin

### on Aurora 5 or Aurora 6

```

$ /opt/spark/bin/spark-submit \
    --name PairwiseAddExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-add-pairwise.py

$ /opt/spark/bin/spark-submit \
    --name WordCountExample \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --master yarn \
    --deploy-mode cluster \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-word-count.py

```

To run in local mode, replace `yarn` with `local[4]` for instance.

Not yet running with our plans (temporary regression):

```

$ /opt/spark/bin/spark-submit \
    --name AveragingExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg.py

$ /opt/spark/bin/spark-submit \
    --name SumExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum.py

$ /opt/spark/bin/spark-submit \
    --name SumMultipleColumnsExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum-multiple.py


$ /opt/spark/bin/spark-submit \
    --name AveragingMultipleColumns5Example \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg-multiple.py

$ /opt/spark/bin/spark-submit \
    --name MultipleOperationsExample \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.com.nec.spark.ncc.path=/opt/nec/ve/bin/ncc \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-multiple-operations.py

```