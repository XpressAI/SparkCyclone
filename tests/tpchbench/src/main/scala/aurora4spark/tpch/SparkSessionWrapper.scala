package aurora4spark.tpch

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {
  implicit lazy val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
}