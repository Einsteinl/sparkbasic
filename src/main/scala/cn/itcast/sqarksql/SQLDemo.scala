package cn.itcast.sqarksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo {

  def main(args: Array[String]): Unit = {

    val conf =new SparkConf().setAppName("SQLDemo").setMaster("local")

    val sc=new SparkContext(conf)

    //创建SQLContext 注意要将sc传进去
    val sqlContext =new SQLContext(sc)

    //设置HDFS的用户名
    System.setProperty("user.name","root")
    //从指定的地址创建RDD
    val lineRDD=sc.textFile("hdfs://mini1:9000/person.txt").map(_.split(" "))

    //创建case class
    //将RDD和case class关联
    val personRDD=lineRDD.map(x => Person(x(0).toInt,x(1),x(2).toInt))

    //导入隐式转换，如果不导入无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF=personRDD.toDF()

    //注册表
    personDF.registerTempTable("t_person")

    //传入SQL
    val df=sqlContext.sql("select * from t_person order by age desc limit 2")

    //将结果以JSON的方式存储到指定的位置
    df.write.json("c://json.txt")

    //停止Spark Context
    sc.stop()


  }

}
//case class 一定要放到外面
case class Person(id: Long,name: String,age: Int)
