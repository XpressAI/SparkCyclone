/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package sparkcyclone.tpch

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

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
  l_returnflag: Long,
  l_linestatus: Long,
  l_shipdate: LocalDate,
  l_commitdate: LocalDate,
  l_receiptdate: LocalDate,
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

object TPCHBenchmark extends SparkSessionWrapper with LazyLogging {
  def createViews(sparkSession: SparkSession, inputDir: String): Unit = {
    import sparkSession.implicits._

    val dfMap = Map[String, SparkContext => DataFrame](
      "customer" -> (sc =>
        sc.textFile(inputDir + "/customer.tbl*")
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
        .toDF()),
      "lineitem" -> (sc =>
        sc.textFile(inputDir + "/lineitem.tbl*")
        .map(_.split('|'))
        .map(p =>
          Lineitem(
            l_orderkey = p(0).trim.toLong,
            l_partkey = p(1).trim.toLong,
            l_suppkey = p(2).trim.toLong,
            l_linenumber = p(3).trim.toLong,
            l_quantity = p(4).trim.toDouble,
            l_extendedprice = p(5).trim.toDouble,
            l_discount = p(6).trim.toDouble,
            l_tax = p(7).trim.toDouble,
            l_returnflag = p(8).trim.toCharArray.apply(0),
            l_linestatus = p(9).trim.toCharArray.apply(0),
            l_shipdate = LocalDate.parse(p(10).trim),
            l_commitdate = LocalDate.parse(p(11).trim),
            l_receiptdate = LocalDate.parse(p(12).trim),
            l_shipinstruct = p(13).trim,
            l_shipmode = p(14).trim,
            l_comment = p(15).trim
          )
        )
        .toDF()),
      "nation" -> (sc =>
        sc.textFile(inputDir + "/nation.tbl*")
        .map(_.split('|'))
        .map(p => Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim))
        .toDF()),
      "region" -> (sc =>
        sc.textFile(inputDir + "/region.tbl*")
        .map(_.split('|'))
        .map(p => Region(p(0).trim.toLong, p(1).trim, p(2).trim))
        .toDF()),
      "orders" -> (sc =>
        sc.textFile(inputDir + "/orders.tbl*")
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
        .toDF()),
      "part" -> (sc =>
        sc.textFile(inputDir + "/part.tbl*")
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
        .toDF()),
      "partsupp" -> (sc =>
        sc.textFile(inputDir + "/partsupp.tbl*")
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
        .toDF()),
      "supplier" -> (sc =>
        sc.textFile(inputDir + "/supplier.tbl*")
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
        .toDF())
    )

    dfMap.foreach { case (key, value) =>
      val sc = sparkSession.sparkContext
      val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      if (!fs.exists(new org.apache.hadoop.fs.Path(s"$key.parquet"))) {
        println(s"Writing ${key}.parquet")

        value(sc).write.parquet(s"${key}.parquet")
      }

      val parquetTable = sparkSession.read.parquet(s"${key}.parquet")
      parquetTable.createOrReplaceTempView(key)
    }
  }

  def main(args: Array[String]): Unit = {

    def getOptions(name: String): List[String] = {
      val full = s"--$name="
      args.toList
        .filter(_.startsWith(full))
        .map(_.drop(full.length))
    }

    val tableDir = getOptions("dbgen").headOption.getOrElse {
      if (args.length >= 1) {
        args(0)
      } else {
        "dbgen"
      }
    }

    createViews(sparkSession, tableDir)

    val queries = Seq(
      (query1 _, 1),
      (query2 _, 2),
      (query3 _, 3),
      (query4 _, 4),
      (query5 _, 5),
      (query6 _, 6),
      (query7 _, 7),
      (query8 _, 8),
      (query9 _, 9),
      (query10 _, 10),
      (query11 _, 11),
      (query12 _, 12),
      (query13 _, 13),
      (query14 _, 14),
      (query15 _, 15),
      (query16 _, 16),
      (query17 _, 17),
      (query18 _, 18),
      (query19 _, 19),
      (query20 _, 20),
      (query21 _, 21),
      (query22 _, 22)
    )

    val toSkip = if (args.length >= 2) {
      args(1).split(",").filter(str => str.forall(Character.isDigit)).map(_.toInt).toSet
    } else {
      Set[Int]()
    }

    println("Spark Config:")
    sparkSession.conf.getAll.foreach { case (key, value) =>
      println(s"$key = $value")
    }

    val toSelect = getOptions("select")
      .flatMap(_.split(",").map(_.toInt))
      .toSet

    val skipPlan = getOptions("plan")
      .map(_.toBoolean)
      .headOption
      .getOrElse(false)

    if (toSelect.nonEmpty) {
      queries
        .filter(q => toSelect.contains(q._2))
        .foreach { case (q, i) =>
          cacheTables(i)
          benchmark(i, q, skipPlan)
        }
    } else
      queries.foreach { case (query, i) =>
        if (!toSkip.contains(i)) {
          cacheTables(i)
          benchmark(i, query, skipPlan)
          //uncacheTables(i)
        }
      }
  }

  val tableMap: Map[Int, Seq[String]] = Map(
    1 -> Seq("lineitem"),
    2 -> Seq("part", "supplier", "partsupp", "nation", "region"),
    3 -> Seq("customer", "orders", "lineitem"),
    4 -> Seq("orders", "lineitem"),
    5 -> Seq("customer", "orders", "lineitem", "supplier", "nation", "region"),
    6 -> Seq("lineitem"),
    7 -> Seq("supplier", "lineitem", "orders", "customer", "nation"),
    8 -> Seq("part", "supplier", "lineitem", "orders", "customer", "nation", "region"),
    9 -> Seq("part", "supplier", "lineitem", "partsupp", "orders", "nation"),
    10 -> Seq("customer", "orders", "lineitem", "nation"),
    11 -> Seq("partsupp", "supplier", "nation"),
    12 -> Seq("orders", "lineitem"),
    13 -> Seq("customer", "orders"),
    14 -> Seq("lineitem", "part"),
    15 -> Seq("lineitem", "supplier"),
    16 -> Seq("partsupp", "part", "supplier"),
    17 -> Seq("lineitem", "part"),
    18 -> Seq("customer", "orders", "lineitem"),
    19 -> Seq("lineitem", "part"),
    20 -> Seq("supplier", "nation", "partsupp", "part", "lineitem"),
    21 -> Seq("supplier", "lineitem", "orders", "nation"),
    22 -> Seq("customer", "orders")
  )

  def cacheTables(i: Int): Unit = {
    for (key <- tableMap(i)) {
      println(s"Caching Table ${key}")

      sparkSession.sql("CACHE TABLE " + key)
      sparkSession.sql("ANALYZE TABLE " + key + " COMPUTE STATISTICS FOR ALL COLUMNS")
    }
  }

  def uncacheTables(i: Int): Unit = {
    for (key <- tableMap(i)) {
      sparkSession.sql("UNCACHE TABLE " + key)
    }
  }

  def benchmark(i: Int, f: SparkSession => (Array[_], DataFrame), skipPlan: Boolean)(implicit sparkSession: SparkSession): Unit = {
    println(s"Running Query${i}")
    logger.info(s"Running Query${i}")

    val start = System.nanoTime()
    val (res, ds) = f(sparkSession)
    val end = System.nanoTime()

    val duration = (end - start).toDouble / 1e9

    println(s"Result returned ${res.length} records.")
    logger.info(s"Result returned ${res.length} records.")

    println(s"Query${i} elapsed: ${duration} s")
    logger.info(s"Query time: ${duration}")

    res.take(10).foreach(println)

    if (!skipPlan) {
      logPlan(ds.queryExecution.executedPlan)
    }
  }

  def query1(sparkSession: SparkSession): (Array[_], DataFrame) = {
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
      limit 1
    """

    val ds = sparkSession.sql(sql)
    val res = ds.limit(1).collect()

    (res, ds)
  }

  private def logPlan(executedPlan: SparkPlan): Unit = println(s"Final plan: ${executedPlan}")

  def query2(sparkSession: SparkSession): (Array[_], DataFrame) = {
    val size = 15
    val pType = "BRASS"
    val region = "EUROPE"

    val sql = s"""
      select /*+ SHUFFLE_HASH(part, supplier, partsupp, nation, region) */
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
          select /*+ SHUFFLE_HASH(supplier, partsupp, nation, region) */
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
    val ds = sparkSession.sql(sql).limit(100)
    val res = ds.collect()

    (res, ds)
  }

  def query3(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.limit(10).collect()


    (res, ds)
  }

  def query4(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query5(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query6(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query7(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query8(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query9(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)

  }

  def query10(sparkSession: SparkSession): (Array[_], DataFrame) = {
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
        and o_orderdate < date '$date' + interval '3' month and l_returnflag = 82
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

    val ds = sparkSession.sql(sql)
    val res = ds.limit(20).collect()

    (res, ds)
  }

  def query11(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query12(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query13(sparkSession: SparkSession): (Array[_], DataFrame) = {
    val word1 = "special"
    val word2 = "requests"

    val sql =
      s"""
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query14(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query15(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    //val sql3 = s"""
    //  drop view revenue$streamId;
    //"""

    sparkSession.sql(sql1)

    val ds = sparkSession.sql(sql2)
    val res = ds.collect()

    (res, ds)
  }

  def query16(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query17(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query18(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.limit(100).collect()

    (res, ds)
  }

  def query19(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query20(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }

  def query21(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.limit(100).collect()

    (res, ds)
  }

  def query22(sparkSession: SparkSession): (Array[_], DataFrame) = {
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

    val ds = sparkSession.sql(sql)
    val res = ds.collect()

    (res, ds)
  }
}
