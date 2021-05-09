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
    --name PairwiseSumExample \
    --master 'local[4]' \
    --jars /opt/aurora4spark/aurora4spark-sql-plugin.jar \
    /opt/aurora4spark/examples/example-sum-pairwise.py
```
