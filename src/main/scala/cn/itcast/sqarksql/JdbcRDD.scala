package cn.itcast.sqarksql


import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *DataFrame 将数据直接写入到mysql数据库中
  */
object JdbcRDD {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("Mysql-Demo")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    //通过并行化创建RDD
    val personRDD=sc.parallelize(Array("3 tom 5","4 jerry 3","5 kitty 6")).map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema=StructType(
      List(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    )

    //将RDD映射到rowRDD
    val rowRDD=personRDD.map(p=>Row(p(0).toInt,p(1).trim,p(2).toInt))

    //将schema信息应用到rowRDD上
    val personDataFrame=sqlContext.createDataFrame(rowRDD,schema)

    //创建Properties存储数据库相关属性
    val prop=new Properties()
    prop.put("user","root")
    prop.put("password","root")

    //将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://mini1:3306/test","test.person",prop)

    //停止SparkContext
    sc.stop()
  }

}
