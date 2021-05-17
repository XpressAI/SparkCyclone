# aurora4spark

Spark Plugin development documentation: [aurora4spark-parent/README.md](aurora4spark-parent/README.md).

## Usage of the plugin

### on `a6`

```
$ source /opt/nec/ve/nlc/2.1.0/bin/nlcvars.sh
$ export PATH=/opt/nec/ve/bin/:$PATH
$ /opt/spark/bin/spark-submit \
    --name AveragingExample \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg.py
$ /opt/spark/bin/spark-submit \
    --name PairwiseAddExample \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-add-pairwise.py
$ /opt/spark/bin/spark-submit \
    --name SumExample \
    --master 'local[4]' \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum.py
$ /opt/spark/bin/spark-submit \
    --name SumMultipleColumnsExample \
    --master 'local[4]' \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum-multiple.py
$ /opt/spark/bin/spark-submit \
    --name AveragingMultipleColumns5Example \
    --master 'local[4]' \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-avg-multiple.py
$ /opt/spark/bin/spark-submit \
    --name MultipleOperationsExample \
    --master 'local[4]' \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-multiple-operations.py
```
