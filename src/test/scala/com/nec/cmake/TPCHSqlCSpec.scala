package com.nec.cmake

import com.eed3si9n.expecty.Expecty.expect
import com.nec.aurora.Aurora
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

@Ignore
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

  def withTpchViews[T](
    configure: SparkSession.Builder => SparkSession.Builder
  )(f: SparkSession => T): T = {
    withSparkSession2(configure) { sparkSession =>
      createViews(sparkSession)
      f(sparkSession)
    }
  }

  "Query 1" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0)
    }
  }
  "Query 2" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0)
    }
  }
  "Query 3" in withTpchViews(configuration) { sparkSession =>
    val segment = "BUILDING"
    val date = "1995-03-15"

    val sql = s"""
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0)
    }
  }
  "Query 4" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0)
    }
  }
  "Query 5" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0)
    }
  }
  "Query 6" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // 123141078.23
    }
  }
  "Query 7" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // FRANCE GERMANY 1995 54639732.73
    }
  }
  "Query 8" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // 1995 0.03
    }
  }
  "Query 9" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // ALGERIA 1998 31342867.24
    }
  }
  "Query 10" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0)
    }
  }
  "Query 11" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // 129760 17538456.86
    }
  }
  "Query 12" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // MAIL 6202 9324
    }
  }
  "Query 13" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) //  9         6641
    }
  }
  "Query 14" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) //  16.38
    }
  }
  "Query 15" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // 8449 Supplier#000008449 Wp34zim9qYFbVctdW 20-469-856-8873 1772627.21
    }
    sparkSession.sql(sql3).show()
  }

  "Query 16" in withTpchViews(configuration) { sparkSession =>
    val brand = "Brand#45"
    val pType = "MEDIUM POLISHED"
    val sizes = Seq(49, 14, 23, 45, 19, 3, 36, 9)

    val sql = s"""
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // Brand#41 MEDIUM BRUSHED TIN 3 28
    }
  }
  "Query 17" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) //  348406.05
    }
  }
  "Query 18" in withTpchViews(configuration) { sparkSession =>
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

    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // Customer#000128120 128120 4722021 1994-04-07 544089.09 323.00
    }
  }
  "Query 19" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // 3083843.05
    }
  }
  "Query 20" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // Supplier#000000020 iybAE,RmTymrZVYaFZva2SH,j
    }
  }
  "Query 21" in withTpchViews(configuration) { sparkSession =>
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
    sparkSession.sql(sql).debugSqlHere { ds =>
      assert(ds.count() > 0) // Supplier#000002829            20
    }
  }
  "Query 22" in withTpchViews(configuration) { sparkSession =>
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
      assert(ds.count() > 0) // 13 888 6737713.99
    }
  }

}
