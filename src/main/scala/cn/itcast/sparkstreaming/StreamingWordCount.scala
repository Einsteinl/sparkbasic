package cn.itcast.sparkstreaming


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 向指定的ip端口发数据，StreamingContext接收到数据将其转换为rdd进行计算
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {

    //设置日志级别
    LoggerLevels.setStreamingLogLevels()

    //这里的local[2]表示开两个executor线程
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamingWordCount")
    val sc=new SparkContext(conf)

    //设置DStrammingContext批次时间间隔为5秒
    val ssc=new StreamingContext(sc,Seconds(5))

    //通过网络读取指定主机端口的数据
    val ds=ssc.socketTextStream("192.168.150.11",8888)
    //DStream是一个特殊的RDD

    val result=ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //打印结果
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
