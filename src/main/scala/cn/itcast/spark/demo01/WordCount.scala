package cn.itcast.spark.demo01

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit ={
    //创建SparkConf()并设置APP名称

    val conf=new SparkConf().setAppName("WC")
      //远程debug
      .setJars(Array("E:\\IDea\\work\\sparkbasic\\target\\spark-basic-1.0-SNAPSHOT.jar"))
          .setMaster("spark://mini1:7077")
    //创建SparkContext，该对象是提交spark App的入口
    val sc=new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    //textFile会产生两个RDD 1.HadoopRDD -> MapPartitionsRDD
    sc.textFile(args(0))
      //产生一个RDD MapPartitionsRDD
      .flatMap(_.split(" "))
      //产生一个RDD MapPartitionsRDD
      .map((_,1))
      //产生一个RDD ShuffledRDD
      .reduceByKey(_+_,1)
      //产生一个RDD MapPartitionsRDD
      .saveAsTextFile(args(1))

    //停止sc，结束该任务
    sc.stop()
  }

}
