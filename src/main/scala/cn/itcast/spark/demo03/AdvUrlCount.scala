package cn.itcast.spark.demo03

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 思路：
  * 1.从数据库中加载规则，也就是要看那些学院的前三页面访问量
  * 2.载入文件，将每行切分为 (url,1)
  * 3.对相同的url进行聚合，得到(url,sum)
  * 4.基于上边所得到的结果，得到(host,url,sum)
  * 5.循环规则库，过滤出匹配的host，对其所有的url进行排序，top3
  */
object AdvUrlCount {

  def main(args: Array[String]): Unit = {

    //从数据库中加载规则
    val arr=Array("java.itcast.cn","php.itcast.cn","net.itcast.cn")

    val conf=new SparkConf().setAppName("AdvUrlCount").setMaster("local")
    val sc=new SparkContext(conf)

    //rdd1将数据切分，元组中放的是(URL,1)
    val rdd1=sc.textFile("c://itcast.log").map(line =>{
      val fields=line.split("\t")
      (fields(1),1)
    })

    //以url为key进行聚合
    val rdd2=rdd1.reduceByKey(_+_)

    val rdd3=rdd2.map(t=>{
      val url=t._1
      val host=new URL(url).getHost
      (host,url,t._2)
    })

    //循环规则库，匹配规则库中host得到的所有url，对其排序
    for(ins <-arr){
      val rdd=rdd3.filter(_._1==ins)
      val result=rdd.sortBy(_._3,false).take(3)
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数，访问日期
      println(result.toBuffer)

    }

    sc.stop()
  }

}
