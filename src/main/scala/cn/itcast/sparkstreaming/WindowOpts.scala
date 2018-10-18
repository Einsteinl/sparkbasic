package cn.itcast.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

object WindowOpts {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf=new SparkConf().setAppName("WindowOpts").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Milliseconds(5000))
    val lines=ssc.socketTextStream("mini1",9999)
    val pairs=lines.flatMap(_.split(" ")).map((_,1))
    //两个时间分别是窗口的长度15秒，10秒窗口移动一次
    val windowedWordCounts=pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(15),Seconds(10))

    windowedWordCounts.print()

//    val a=windowedWordCounts.map(_._2).reduce(_+_)
//    a.foreachRDD(rdd =>{
//      println(rdd.take(0))
//    })
//    a.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
