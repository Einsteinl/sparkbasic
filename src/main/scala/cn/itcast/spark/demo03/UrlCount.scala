package cn.itcast.spark.demo03

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求：学员访问传智播客官网各个学院的页面，学院有：java，.net,PHP等等
  * 现在，需要将各个学院页面的点击量为前三的页面提取出来，看看学员主要关心什么，好做改善
  */
object UrlCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UrlCount").setMaster("local")
    val sc=new SparkContext(conf)
    //加载文件，切分结果 (url,1)
    val url_1=sc.textFile("c://itcast.log").map(line =>{
      val fields=line.split("\t")
      (fields(1),1)
    })

    //相同的页面进行聚合
    val url_sum=url_1.reduceByKey(_+_)

    //将url中的host提取出来
    val host_url_sum=url_sum.map(it =>{
      val url=it._1
      val host=new URL(url).getHost
      (host,url,it._2)
    })

    //按照host进行分组，对页面访问量sum进行排序
    val result=host_url_sum.groupBy(_._1).mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(3)
    })

    println(result.collect().toBuffer)

    sc.stop()
  }

}
