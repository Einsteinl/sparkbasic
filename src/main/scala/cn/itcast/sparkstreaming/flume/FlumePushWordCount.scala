package cn.itcast.sparkstreaming.flume

import cn.itcast.sparkstreaming.LoggerLevels
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * flume 采集到的数据直接传到woker的ip端口上，woker 中的executor去消费它
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf=new SparkConf().setAppName("FlumeWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))

    //推送方式：flume向spark发送数据
    val flumeStream=FlumeUtils.createStream(ssc,"10.2.129.186",8888)

    //flume中的数据通过event.getBody()才能拿到真正的内容
    val words=flumeStream.flatMap(x => new String(x.event.getBody().array()).split(" ")).map((_,1))

    val results=words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
