package cn.itcast.spark.demo_partitoner

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 增加了cache功能
  */
object UrlCountPartitionCache {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UrlCountPartitionCache").setMaster("local")
    val sc=new SparkContext(conf)

    val rdd1=sc.textFile("c://itcast.log").map(line =>{
      val fields=line.split("\t")
      (fields(1),1)
    })

    val rdd2=rdd1.reduceByKey(_+_)

    val rdd3=rdd2.map(x =>{
      val url=x._1
      val host=new URL(url).getHost
      (host,(url,x._2))
    }).cache()//cache会将数据缓存到内存当中，cache是一个Transformation，lazy

    val rdd4=rdd3.map(x =>{
      x._1
    })
    val ins=rdd4.distinct().collect()

    val hostParitioner=new MyPartitioner(ins)

    val rdd5=rdd3.partitionBy(hostParitioner).mapPartitions(it =>{
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    println(rdd5.collect().toBuffer)


  }

}

class MyPartitioner(ins: Array[String]) extends Partitioner{

  val map=new mutable.HashMap[String,Int]()

  var count=0
  for(x <-ins){
    map +=(x -> count)
    count +=1
  }

  override def numPartitions: Int = ins.length



  override def getPartition(key: Any): Int = {
    map.getOrElse(key.toString,0)
  }
}
