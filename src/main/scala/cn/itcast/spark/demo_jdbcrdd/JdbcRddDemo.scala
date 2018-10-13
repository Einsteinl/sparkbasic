package cn.itcast.spark.demo_jdbcrdd

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark 提供了jdbcRdd，可以直接连接数据库
  */
object JdbcRddDemo {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("JdbcRddDemo").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val connection=() =>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","leishao")
    }
    val jdbcRDD=new JdbcRDD(
      sc,
      connection,
      "select * from ta where id >= ? and id <= ?",
      1,4,2,
      r => {
        val id=r.getInt(1)
        val code=r.getString(2)
        (id,code)
      }
    )

    val jrdd=jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }

}
