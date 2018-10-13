package cn.itcast.spark.demo_partitoner

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  *主要实现如何将一个rdd的数据分区，自定义分区规则，因为默认的HashPartitoner存在Hash碰撞的情况
  * 1.将url从文件中读出来，切分为(url,1)
  * 2.进行聚合。(url,sum)
  * 3.在上个结果的基础上，抽出每个url的host,结果为(host,url,sum)
  * 4.之后按照自定义的分区对上个RDD进行分区，并在每个分区内部进行排序
  * 5.结果保存
  */
object UrlCountPartition {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc=new SparkContext(conf)

    //读入文件，将每行转为 (url,1)
    val rdd1=sc.textFile("c://itcast.log").map(line =>{
      val fields=line.split("\t")
      (fields(1),1)
    })

    val rdd2=rdd1.reduceByKey(_+_)

    val rdd3=rdd2.map(t =>{
      val url=t._1
      val host=new URL(url).getHost
      (host,(url,t._2))
    })

    val ints=rdd3.map(_._1).distinct().collect()

    val hostParitioner=new HostParitioner(ints)

    //将数据按照host分区，并对每一个分区进行排序
    val rdd4=rdd3.partitionBy(hostParitioner).mapPartitions(it =>{
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })

    rdd4.saveAsTextFile("c://out1")

    sc.stop()
  }

}

/**
  * 决定数据到那个分区里面
  */

class HostParitioner(ins :Array[String]) extends Partitioner{

  //分区的主构造器：将串进来的ins，一一遍历出来放入map中，并在对应的key中，为其匹配上分区号，从0开始

  //存放host，用于传进来的host分配到对应的分区中(get方法)
  val parMap=new mutable.HashMap[String,Int]()
  //分区号
  var count=0

  for(i <- ins){
    parMap += (i -> count)
    count +=1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}
