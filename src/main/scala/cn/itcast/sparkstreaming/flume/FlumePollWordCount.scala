package cn.itcast.sparkstreaming.flume

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 从flume节点拿去数据，这个用的比push要多
  */
object FlumePollWordCount {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("FlumePollWordCount")

    val ssc=new StreamingContext(conf,Seconds(5))

    //从flume中拉取数据(flume的地址),可以放多个flume地址，从多个flume节点拉取数据
    val address=Seq(new InetSocketAddress("192.168.150.11",8888))

    val flumeStream=FlumeUtils.createPollingStream(ssc,address,StorageLevel.MEMORY_AND_DISK)

    val words=flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ")).map((_,1))

    val results=words.reduceByKey(_+_)

    results.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
