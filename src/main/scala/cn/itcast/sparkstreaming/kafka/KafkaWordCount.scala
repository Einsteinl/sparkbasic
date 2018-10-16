package cn.itcast.sparkstreaming.kafka

import cn.itcast.sparkstreaming.LoggerLevels
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {

  val updateFunc=(iter: Iterator[(String,Seq[Int],Option[Int])]) =>{
    //iter.flatMap(it => Some(it._2.sum+it._3.getOrElse(0)).map(x =>(it._1,x)))
    iter.flatMap{case(x,y,z) => Some(y.sum+z.getOrElse(0)).map(i=>(x,i))}

  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum,group,topics,numThreads)=args
    val sparkConf=new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("c://ck2")

    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap
    val data=KafkaUtils.createStream(ssc,zkQuorum,group,topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    val words=data.map(_._2).flatMap(_.split(" "))
    val wordCounts=words.map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
