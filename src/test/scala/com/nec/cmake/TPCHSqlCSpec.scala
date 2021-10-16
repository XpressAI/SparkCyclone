package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.native.NativeEvaluator.ExecutorPluginManagedEvaluator
import com.nec.spark.planning.{NativeAggregationEvaluationPlan, VERewriteStrategy}
import com.nec.spark.{Aurora4SparkExecutorPlugin, AuroraSqlPlugin, SparkAdditions}
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.internal.SQLConf.CODEGEN_FALLBACK
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Ignore}
import java.io.File

import org.apache.spark.sql.types.{DataTypes, DoubleType, LongType, StructField, StructType}

// TPC-H table schemas
case class Customer(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String
)

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_suppkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String
)

case class Nation(n_nationkey: Long, n_name: String, n_regionkey: Long, n_comment: String)

case class Order(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Long,
  o_comment: String
)

case class Part(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Long,
  p_container: String,
  p_retailprice: Double,
  p_comment: String
)

case class Partsupp(
  ps_partkey: Long,
  ps_suppkey: Long,
  ps_availqty: Long,
  ps_supplycost: Double,
  ps_comment: String
)

case class Region(r_regionkey: Long, r_name: String, r_comment: String)

case class Supplier(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String
)

object TPCHSqlSpec {

  def VeConfiguration: SparkSession.Builder => SparkSession.Builder = {
    _.config(CODEGEN_FALLBACK.key, value = false)
      .config("spark.sql.codegen.comments", value = true)
      .config("spark.plugins", classOf[AuroraSqlPlugin].getCanonicalName)
      .withExtensions(sse =>
        sse.injectPlannerStrategy(_ => new VERewriteStrategy(ExecutorPluginManagedEvaluator))
      )
  }

}

class TPCHSqlCSpec
  extends AnyFreeSpec
  with BeforeAndAfter
  with BeforeAndAfterAll
  with SparkAdditions
  with Matchers
  with LazyLogging {

  private var initialized = false

  def configuration: SparkSession.Builder => SparkSession.Builder =
    DynamicCSqlExpressionEvaluationSpec.DefaultConfiguration
  val resultsDir = "src/test/resources/com/nec/spark/results/"

  def createViews(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    val sc = sparkSession.sparkContext
    val inputDir = "src/test/resources/dbgen"

    val dfMap = Map(
      "customer" -> sc
        .textFile(inputDir + "/customer.tbl*")
        .map(_.split('|'))
        .map(p =>
          Customer(
            p(0).trim.toLong,
            p(1).trim,
            p(2).trim,
            p(3).trim.toLong,
            p(4).trim,
            p(5).trim.toDouble,
            p(6).trim,
            p(7).trim
          )
        )
        .toDF(),
      "lineitem" -> sc
        .textFile(inputDir + "/lineitem.tbl*")
        .map(_.split('|'))
        .map(p =>
          Lineitem(
            p(0).trim.toLong,
            p(1).trim.toLong,
            p(2).trim.toLong,
            p(3).trim.toLong,
            p(4).trim.toDouble,
            p(5).trim.toDouble,
            p(6).trim.toDouble,
            p(7).trim.toDouble,
            p(8).trim,
            p(9).trim,
            p(10).trim,
            p(11).trim,
            p(12).trim,
            p(13).trim,
            p(14).trim,
            p(15).trim
          )
        )
        .toDF(),
      "nation" -> sc
        .textFile(inputDir + "/nation.tbl*")
        .map(_.split('|'))
        .map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim))
        .toDF(),
      "region" -> sc
        .textFile(inputDir + "/region.tbl*")
        .map(_.split('|'))
        .map(p => Region(p(0).trim.toLong, p(1).trim, p(2).trim))
        .toDF(),
      "orders" -> sc
        .textFile(inputDir + "/orders.tbl*")
        .map(_.split('|'))
        .map(p =>
          Order(
            p(0).trim.toLong,
            p(1).trim.toLong,
            p(2).trim,
            p(3).trim.toDouble,
            p(4).trim,
            p(5).trim,
            p(6).trim,
            p(7).trim.toLong,
            p(8).trim
          )
        )
        .toDF(),
      "part" -> sc
        .textFile(inputDir + "/part.tbl*")
        .map(_.split('|'))
        .map(p =>
          Part(
            p(0).trim.toLong,
            p(1).trim,
            p(2).trim,
            p(3).trim,
            p(4).trim,
            p(5).trim.toLong,
            p(6).trim,
            p(7).trim.toDouble,
            p(8).trim
          )
        )
        .toDF(),
      "partsupp" -> sc
        .textFile(inputDir + "/partsupp.tbl*")
        .map(_.split('|'))
        .map(p =>
          Partsupp(
            p(0).trim.toLong,
            p(1).trim.toLong,
            p(2).trim.toLong,
            p(3).trim.toDouble,
            p(4).trim
          )
        )
        .toDF(),
      "supplier" -> sc
        .textFile(inputDir + "/supplier.tbl*")
        .map(_.split('|'))
        .map(p =>
          Supplier(
            p(0).trim.toLong,
            p(1).trim,
            p(2).trim,
            p(3).trim.toLong,
            p(4).trim,
            p(5).trim.toDouble,
            p(6).trim
          )
        )
        .toDF()
    )

    dfMap.foreach { case (key, value) =>
      value.createOrReplaceTempView(key)
    }
  }

  implicit class RichDataSet[T](val dataSet: Dataset[T]) {
    def ensureCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("CEvaluation"))
      dataSet
    }

    def ensureNewCEvaluating(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(
        thePlan
          .toString()
          .contains(
            NativeAggregationEvaluationPlan.getClass.getSimpleName.replaceAllLiterally("$", "")
          )
      )
      dataSet
    }

    def ensureJoinPlanEvaluated(): Dataset[T] = {
      val thePlan = dataSet.queryExecution.executedPlan
      expect(thePlan.toString().contains("GeneratedJoinPlan"))
      dataSet
    }

    def debugSqlHere[V](f: Dataset[T] => V): V = {
      try f(dataSet)
      catch {
        case e: Throwable =>
          logger.info(s"${dataSet.queryExecution.executedPlan}; ${e}", e)
          throw e
      }
    }
  }

  def withTpchViews[T](appName: String, configure: SparkSession.Builder => SparkSession.Builder)(
    f: SparkSession => T
  ): Unit =
    appName in {
      withSparkSession2(configure.compose[SparkSession.Builder](_.appName(appName))) {
        sparkSession =>
          createViews(sparkSession)
          f(sparkSession)
      }
    }

  withTpchViews("Query 1", configuration) { sparkSession =>
    import sparkSession.implicits._
    val delta = 90
    val sql = s"""
      select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
        sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
      from
        lineitem
      where
        l_shipdate <= date '1998-12-01' - interval '$delta' day
      group by l_returnflag, l_linestatus
      order by l_returnflag, l_linestatus
    """

    sparkSession.sql(sql).debugSqlHere { ds =>
      type Tpe = (String, String, Double, Double, Double, Double, Double, Double, Double, Long)
      assert(
        ds.as[(String, String, Double, Double, Double, Double, Double, Double, Double, Long)]
          .collect()
          .toList
          .sortBy(v => (v._1, v._2)) == List[Tpe](
          (
            "A",
            "F",
            3.7734107e7,
            5.658655440073002e10,
            5.3758257134869965e10,
            5.590906522282772e10,
            25.522005853257337,
            38273.12973462168,
            0.04998529583840328,
            1478493
          ),
          (
            "N",
            "F",
            991417.0,
            1.4875047103799999e9,
            1.4130821680541e9,
            1.4696492231943748e9,
            25.516471920522985,
            38284.4677608483,
            0.050093426674216374,
            38854
          ),
          (
            "N",
            "O",
            7.447604e7,
            1.1170172969773961e11,
            1.0611823030760545e11,
            1.1036704387249672e11,
            25.50222676958499,
            38249.11798890814,
            0.049996586053750215,
            2920374
          ),
          (
            "R",
            "F",
            3.7719753e7,
            5.656804138090005e10,
            5.3741292684604004e10,
            5.588961911983193e10,
            25.50579361269077,
            38250.85462609969,
            0.05000940583013268,
            1478870
          )
        ).sortBy(v => (v._1, v._2))
      )
    }
  }
  withTpchViews("Query 2", configuration) { sparkSession =>
    import sparkSession.implicits._
    val size = 15
    val pType = "BRASS"
    val region = "EUROPE"

    val sql = s"""
      select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
      from
        part,
        supplier,
        partsupp,
        nation,
        region
      where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = $size
        and p_type like '%$pType'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '$region'
        and ps_supplycost = (
          select
            min(ps_supplycost)
          from
            partsupp,
            supplier,
            nation,
            region
          where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = '$region'
        )
        order by
          s_acctbal desc,
          n_name,
          s_name,
          p_partkey
    """
    val resultSchema = StructType(
      Seq(
        StructField("_0", DoubleType),
        StructField("_1", DataTypes.StringType),
        StructField("_2", DataTypes.StringType),
        StructField("_3", LongType),
        StructField("_4", DataTypes.StringType),
        StructField("_5", DataTypes.StringType),
        StructField("_6", DataTypes.StringType),
        StructField("_7", DataTypes.StringType)
      )
    )
    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query2.csv")
      .as[(Double, String, String, Long, String, String, String, String)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(Double, String, String, Long, String, String, String, String)]
          .collect()
          .toList
          .sorted == result.sorted
      )
    }
  }
  withTpchViews("Query 3", configuration) { sparkSession =>
    import sparkSession.implicits._
    val segment = "BUILDING"
    val date = "1995-03-15"

    val sql =
      s"""
      select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
      from
        customer,
        orders,
        lineitem
      where
        c_mktsegment = '$segment'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '$date'
        and l_shipdate > date '$date'
      group by
        l_orderkey,
        o_orderdate,
        o_shippriority
      order by
        revenue desc,
        o_orderdate
    """

    val resultSchema = StructType(
      Seq(
        StructField("_0", LongType),
        StructField("_1", DoubleType),
        StructField("_2", DataTypes.StringType),
        StructField("_3", DataTypes.LongType)
      )
    )

    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query3.csv")
      .as[(Long, Double, String, Long)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(Long, Double, String, Long)].collect().toList.sorted == result.sorted)
    }
  }

  withTpchViews("Query 4", configuration) { sparkSession =>
    import sparkSession.implicits._

    val date = "1993-07-01"

    val sql = s"""
      select
        o_orderpriority,
        count(*) as order_count
      from
        orders
      where
        o_orderdate >= date '$date'
        and o_orderdate < date '$date' + interval '3' month
        and exists (
          select *
          from
            lineitem
          where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
        )
      group by
        o_orderpriority
      order by
        o_orderpriority;
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, Long)].collect().toList.sorted == List(
          ("1-URGENT", 10594),
          ("2-HIGH", 10476),
          ("3-MEDIUM", 10410),
          ("4-NOT SPECIFIED", 10556),
          ("5-LOW", 10487)
        ).sorted
      )
    }
  }
  withTpchViews("Query 5", configuration) { sparkSession =>
    import sparkSession.implicits._

    val region = "ASIA"
    val date = "1994-01-01"

    val sql = s"""
      select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
      from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
      where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '$region'
        and o_orderdate >= date '$date'
        and o_orderdate < date '$date' + interval '1' year
      group by
        n_name
      order by
        revenue desc
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, Double)].collect().toList.sorted == List(
          ("INDONESIA", 5.5502041169699915e7),
          ("VIETNAM", 5.529508699669991e7),
          ("CHINA", 5.372449425660001e7),
          ("INDIA", 5.2035512000199996e7),
          ("JAPAN", 4.5410175695400015e7)
        ).sorted
      )
    }
  }
  withTpchViews("Query 6", configuration) { sparkSession =>
    import sparkSession.implicits._
    val date = "1994-01-01"
    val discount = 0.06
    val quantity = 24

    val sql = s"""
      select
        sum(l_extendedprice*l_discount) as revenue
      from
        lineitem
      where
        l_shipdate >= date '$date'
        and l_shipdate < date '$date' + interval '1' year
        and l_discount between $discount - 0.01
        and $discount + 0.01
        and l_quantity < $quantity
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      ds.as[scala.Double].collect.toList.head shouldEqual 1.2314107822829995e8.+-(
        0.00001
      ) // We get here 1.2314107822829895E8 from our code
    }
  }
  withTpchViews("Query 7", configuration) { sparkSession =>
    import sparkSession.implicits._

    val nation1 = "FRANCE"
    val nation2 = "GERMANY"

    val sql = s"""
      select
        supp_nation,
        cust_nation,
        l_year,
        sum(volume) as revenue
      from (
        select
          n1.n_name as supp_nation,
          n2.n_name as cust_nation,
          extract(year from l_shipdate) as l_year,
          l_extendedprice * (1 - l_discount) as volume
        from
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2
        where
          s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
            (n1.n_name = '$nation1' and n2.n_name = '$nation2')
            or (n1.n_name = '$nation2' and n2.n_name = '$nation1')
          )
          and l_shipdate between date '1995-01-01' and date '1996-12-31' ) as shipping
      group by
        supp_nation,
        cust_nation,
        l_year
      order by
        supp_nation,
        cust_nation,
        l_year
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, String, Int, Double)].collect().toList.sorted == List(
          ("FRANCE", "GERMANY", 1995, 5.463973273360003e7),
          ("FRANCE", "GERMANY", 1996, 5.463308330760005e7),
          ("GERMANY", "FRANCE", 1995, 5.253174666970001e7),
          ("GERMANY", "FRANCE", 1996, 5.2520549022399865e7)
        )
      ) // FRANCE GERMANY 1995 54639732.7.sorted3
    }
  }
  //Fails
  withTpchViews("Query 8", configuration) { sparkSession =>
    import sparkSession.implicits._
    val nation = "BRAZIL"
    val region = "AMERICA"
    val pType = "ECONOMY ANODIZED STEEL"

    val sql = s"""
      select
        o_year,
        sum(
          case when nation = '$nation'
            then volume
            else 0
          end
        ) / sum(volume) as mkt_share
      from (
        select
          extract(year from o_orderdate) as o_year,
          l_extendedprice * (1-l_discount) as volume,
          n2.n_name as nation
        from
          part,
          supplier,
          lineitem,
          orders,
          customer,
          nation n1,
          nation n2,
          region
        where
          p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and r_name = '$region'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01'
          and date '1996-12-31'
          and p_type = '$pType'
       ) as all_nations
     group by
      o_year
    order by
      o_year
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(Long, Double)].collect.toList.sorted == List(
          (1995, 0.03443589040665487),
          (1996, 0.041485521293530316)
        ).sorted
      )
    }
  }
  withTpchViews("Query 9", configuration) { sparkSession =>
    import sparkSession.implicits._

    val color = "green"

    val sql = s"""
      select
        nation,
        o_year,
        sum(amount) as sum_profit
      from (
        select
          n_name as nation,
          extract(year from o_orderdate) as o_year,
          l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
          part,
          supplier,
          lineitem,
          partsupp,
          orders,
          nation
        where
          s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%$color%'
      ) as profit
      group by
        nation,
        o_year
      order by
        nation,
        o_year desc
    """

    val resultSchema = StructType(
      Seq(
        StructField("_0", DataTypes.StringType),
        StructField("_1", DataTypes.IntegerType),
        StructField("_2", DoubleType)
      )
    )
    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query9.csv")
      .as[(String, Int, Double)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(String, Int, Double)].collect().toList.sorted == result.sorted)
    }
  }
  withTpchViews("Query 10", configuration) { sparkSession =>
    import sparkSession.implicits._
    val date = "1993-10-01"

    val sql = s"""
      select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
      from
        customer,
        orders,
        lineitem,
        nation
      where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '$date'
        and o_orderdate < date '$date' + interval '3' month and l_returnflag = 'R'
        and c_nationkey = n_nationkey
      group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
      order by
        revenue desc
    """

    val resultSchema = StructType(
      Seq(
        StructField("_0", LongType),
        StructField("_1", DataTypes.StringType),
        StructField("_2", DoubleType),
        StructField("_3", DoubleType),
        StructField("_4", DataTypes.StringType),
        StructField("_5", DataTypes.StringType),
        StructField("_6", DataTypes.StringType),
        StructField("_7", DataTypes.StringType)
      )
    )
    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query10.csv")
      .as[(Long, String, Double, Double, String, String, String, String)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(Long, String, Double, Double, String, String, String, String)]
          .collect()
          .toList
          .sorted == result.sorted
      )
    }
  }
  withTpchViews("Query 11", configuration) { sparkSession =>
    import sparkSession.implicits._

    val nation = "GERMANY"
    val fraction = 0.0001

    val sql = s"""
      select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
      from
        partsupp,
        supplier,
        nation
      where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = '$nation'
      group by
        ps_partkey having sum(ps_supplycost * ps_availqty) > (
          select
            sum(ps_supplycost * ps_availqty) * $fraction
          from
            partsupp,
            supplier,
            nation
          where
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = '$nation'
        )
      order by
        value desc
    """

    val resultSchema = StructType(Seq(StructField("_0", LongType), StructField("_1", DoubleType)))
    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query11.csv")
      .as[(Long, Double)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(Long, Double)].collect().toList.sorted == result.sorted)
    }
  }
  //This doesn't work.
  withTpchViews("Query 12", configuration) { sparkSession =>
    import sparkSession.implicits._
    val shipMode1 = "MAIL"
    val shipMode2 = "SHIP"
    val date = "1994-01-01"

    val sql = s"""
      select
        l_shipmode,
        sum(
          case
            when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH'
            then 1
            else 0
          end
        ) as high_line_count,
        sum(
          case
            when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH'
            then 1
            else 0
          end
        ) as low_line_count
      from
        orders,
        lineitem
      where
        o_orderkey = l_orderkey
        and l_shipmode in ('$shipMode1', '$shipMode2')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '$date'
        and l_receiptdate < date '$date' + interval '1' year
      group by l_shipmode
      order by l_shipmode
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, BigInt, BigInt)].collect.toList.sorted == List(
          ("MAIL", 6202, 9324),
          ("SHIP", 6200, 9262)
        ).sorted
      )
    }
  }
  withTpchViews("Query 13", configuration) { sparkSession =>
    import sparkSession.implicits._

    val word1 = "special"
    val word2 = "requests"

    val sql = s"""
      select
        c_count,
        count(*) as custdist
      from (
        select
          c_custkey,
          count(o_orderkey)
        from
          customer
        left outer join
          orders on c_custkey = o_custkey
          and o_comment not like '%$word1%$word2%'
        group by c_custkey
      ) as c_orders (c_custkey, c_count)
      group by
        c_count
      order by
        custdist desc,
        c_count desc
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(Long, Long)].collect.toList.sorted == List(
          (0, 50005),
          (9, 6641),
          (10, 6532),
          (11, 6014),
          (8, 5937),
          (12, 5639),
          (13, 5024),
          (19, 4793),
          (7, 4687),
          (17, 4587),
          (18, 4529),
          (20, 4516),
          (15, 4505),
          (14, 4446),
          (16, 4273),
          (21, 4190),
          (22, 3623),
          (6, 3265),
          (23, 3225),
          (24, 2742),
          (25, 2086),
          (5, 1948),
          (26, 1612),
          (27, 1179),
          (4, 1007),
          (28, 893),
          (29, 593),
          (3, 415),
          (30, 376),
          (31, 226),
          (32, 148),
          (2, 134),
          (33, 75),
          (34, 50),
          (35, 37),
          (1, 17),
          (36, 14),
          (38, 5),
          (37, 5),
          (40, 4),
          (41, 2),
          (39, 1)
        ).sorted
      )
    }
  }
  //Fails
  withTpchViews("Query 14", configuration) { sparkSession =>
    import sparkSession.implicits._
    val date = "1995-09-01"

    val sql = s"""
      select
        100.00 * sum(
          case
            when p_type like 'PROMO%'
            then l_extendedprice * (1 - l_discount)
            else 0
          end
        ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
      from
        lineitem,
        part
      where
        l_partkey = p_partkey
        and l_shipdate >= date '$date'
        and l_shipdate < date '$date' + interval '1' month
    """

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[Double].collect.toList.sorted == List(16.38077862639553)) //  16.3.sorted8
    }
  }
  withTpchViews("Query 15", configuration) { sparkSession =>
    import sparkSession.implicits._
    val streamId = "1"
    val date = "1996-01-01"

    val sql1 = s"""
      create temp view revenue$streamId (supplier_no, total_revenue) as
      select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
      from
        lineitem
      where
        l_shipdate >= date '$date'
        and l_shipdate < date '$date' + interval '3' month
      group by
        l_suppkey"""
    val sql2 = s"""
      select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
      from
        supplier,
        revenue$streamId
      where
        s_suppkey = supplier_no
        and total_revenue = (
          select
            max(total_revenue)
          from
            revenue$streamId
        )
      order by
        s_suppkey"""

    val sql3 = s"""
      drop view revenue$streamId;
    """

    sparkSession.sql(sql1).show()
    sparkSession.sql(sql2).debugSqlHere { ds =>
      assert(
        ds.as[(Long, String, String, String, Double)].collect.toList.sorted == List(
          (8449, "Supplier#000008449", "Wp34zim9qYFbVctdW", "20-469-856-8873", 1772627.2087000003)
        ).sorted
      )
    }
    sparkSession.sql(sql3).show()
  }

  withTpchViews("Query 16", configuration) { sparkSession =>
    import sparkSession.implicits._
    val brand = "Brand#45"
    val pType = "MEDIUM POLISHED"
    val sizes = Seq(49, 14, 23, 45, 19, 3, 36, 9)

    val sql =
      s"""
      select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
      from
        partsupp,
        part
      where
        p_partkey = ps_partkey
        and p_brand <> '$brand'
        and p_type not like '$pType%'
        and p_size in (${sizes.mkString(",")})
        and ps_suppkey not in (
          select
            s_suppkey
          from
            supplier
          where
            s_comment like '%Customer%Complaints%'
          )
      group by
        p_brand,
        p_type,
        p_size
      order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size
    """
    val resultSchema = StructType(
      Seq(
        StructField("_0", DataTypes.StringType),
        StructField("_1", DataTypes.StringType),
        StructField("_2", LongType),
        StructField("_3", LongType)
      )
    )

    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query16.csv")
      .as[(String, String, Long, Long)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(String, String, Long, Long)].collect.toList.sorted == result.sorted)
    }
  }
  withTpchViews("Query 17", configuration) { sparkSession =>
    import sparkSession.implicits._
    val brand = "Brand#23"
    val container = "MED BOX"

    val sql = s"""
      select
        (sum(l_extendedprice) / 7.0) as avg_yearly
      from
        lineitem,
        part
      where
        p_partkey = l_partkey
        and p_brand = '$brand'
        and p_container = '$container'
        and l_quantity < (
          select
            0.2 * avg(l_quantity)
          from
            lineitem
          where
            l_partkey = p_partkey
        )
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[Double].collect().toList.sorted == List(348406.05428571434)) //  348406.0.sorted5
    }
  }
  withTpchViews("Query 18", configuration) { sparkSession =>
    import sparkSession.implicits._
    val quantity = 300

    val sql = s"""
      select
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice,
        sum(l_quantity)
      from
        customer,
        orders,
        lineitem
      where
        o_orderkey in (
          select
            l_orderkey
          from
            lineitem
          group by
            l_orderkey
          having
            sum(l_quantity) > $quantity
        )
        and c_custkey = o_custkey
        and o_orderkey = l_orderkey
      group by
        c_name,
        c_custkey,
        o_orderkey,
        o_orderdate,
        o_totalprice
      order by
        o_totalprice desc,
        o_orderdate
    """

    val resultSchema = StructType(
      Seq(
        StructField("_0", DataTypes.StringType),
        StructField("_1", DataTypes.LongType),
        StructField("_2", LongType),
        StructField("_3", DataTypes.StringType),
        StructField("_4", DoubleType),
        StructField("_5", DoubleType)
      )
    )

    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query18.csv")
      .as[(String, Long, Long, String, Double, Double)]
      .collect()
      .toList

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, Long, Long, String, Double, Double)]
          .collect()
          .toList
          .sorted == result
      ) // Customer#000128120 128120 4722021 1994-04-07 544089.09 323.0.sorted0
    }
  }
  withTpchViews("Query 19", configuration) { sparkSession =>
    import sparkSession.implicits._
    val brand1 = "Brand#12"
    val quantity1 = 1

    val brand2 = "Brand#23"
    val quantity2 = 10

    val brand3 = "Brand#34"
    val quantity3 = 20

    val sql = s"""
      select
        sum(l_extendedprice * (1 - l_discount) ) as revenue
      from
        lineitem,
        part
      where (
        p_partkey = l_partkey
        and p_brand = '$brand1'
        and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= $quantity1
        and l_quantity <= $quantity1 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or (
        p_partkey = l_partkey
        and p_brand = '$brand2'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= $quantity2
        and l_quantity <= $quantity2 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
      )
      or (
        p_partkey = l_partkey
        and p_brand = '$brand3'
        and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= $quantity3
        and l_quantity <= $quantity3 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
      )
    """

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[Double].collect.toList.sorted == List(3083843.057799999)) // 3083843.0.sorted5
    }
  }
  withTpchViews("Query 20", configuration) { sparkSession =>
    import sparkSession.implicits._
    val color = "forest"
    val date = "1994-01-01"
    val nation = "CANADA"

    val sql = s"""
      select
        s_name,
        s_address
      from
        supplier,
        nation
      where
        s_suppkey in (
          select
            ps_suppkey
          from
            partsupp
          where
            ps_partkey in (
              select
                p_partkey
              from
                part
              where p_name like '$color%'
            )
            and ps_availqty > (
              select
                0.5 * sum(l_quantity)
              from
                lineitem
              where
                l_partkey = ps_partkey
                and l_suppkey = ps_suppkey
                and l_shipdate >= date('$date')
                and l_shipdate < date('$date') + interval '1' year
            )
        )
        and s_nationkey = n_nationkey
        and n_name = '$nation'
      order by
        s_name
    """
    val resultSchema = StructType(
      Seq(StructField("_0", DataTypes.StringType), StructField("_1", DataTypes.StringType))
    )

    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query20.csv")
      .as[(String, String)]
      .collect()
      .toList
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(String, String)].collect().toList.sorted == result.sorted)
    }
  }
  withTpchViews("Query 21", configuration) { sparkSession =>
    import sparkSession.implicits._
    val nation = "SAUDI ARABIA"

    val sql = s"""
      select
        s_name,
        count(*) as numwait
      from
        supplier,
        lineitem l1,
        orders,
        nation
      where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
          select *
          from
            lineitem l2
          where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
          select *
          from
            lineitem l3
          where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = '$nation'
      group by
        s_name
      order by
        numwait desc,
        s_name
    """
    val resultSchema = StructType(
      Seq(StructField("_0", DataTypes.StringType), StructField("_1", DataTypes.LongType))
    )

    val result = sparkSession.read
      .schema(resultSchema)
      .csv(resultsDir + "Query21.csv")
      .as[(String, Long)]
      .collect()
      .toList
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.as[(String, Long)].collect.toList.sorted == result.sorted)
    }
  }
  withTpchViews("Query 22", configuration) { sparkSession =>
    import sparkSession.implicits._
    val items = Seq("'13'", "'31'", "'23'", "'29'", "'30'", "'18'", "'17'")

    val sql = s"""
      select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
      from (
        select
          substring(c_phone from 1 for 2) as cntrycode,
          c_acctbal
        from
          customer
        where
          substring(c_phone from 1 for 2) in (${items.mkString(",")})
          and c_acctbal > (
            select
              avg(c_acctbal)
            from
              customer
            where
              c_acctbal > 0.00
              and substring (c_phone from 1 for 2) in (${items.mkString(",")})
          )
          and not exists (
            select *
            from
              orders
            where
              o_custkey = c_custkey
          )
      ) as custsale
      group by
        cntrycode
      order by
        cntrycode
    """
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(
        ds.as[(String, Long, Double)].collect.toList.sorted == List(
          ("13", 888, 6737713.989999999),
          ("17", 861, 6460573.719999993),
          ("18", 964, 7236687.399999998),
          ("23", 892, 6701457.950000002),
          ("29", 948, 7158866.629999999),
          ("30", 909, 6808436.129999996),
          ("31", 922, 6806670.179999999)
        )
      ) // 13 888 6737713.9.sorted9
    }
  }
}
