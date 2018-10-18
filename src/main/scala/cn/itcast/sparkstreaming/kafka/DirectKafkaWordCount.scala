package cn.itcast.sparkstreaming.kafka

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectKafkaWordCount {

  /*  def dealLine(line: String): String = {
    val list = line.split(',').toList
//    val list = AnalysisUtil.dealString(line, ',', '"')// 把dealString函数当做split即可
    list.get(0).substring(0, 10) + "-" + list.get(26)
  }*/
  //处理消息
  def processRdd(rdd: RDD[(String,String)]) :Unit={
    val lines=rdd.map(_._2)
    val words=lines.map(_.split(" "))
    val wordCounts=words.map(x => (x,1L)).reduceByKey(_+_)
    wordCounts.foreach(println)
  }



  def main(args: Array[String]): Unit = {

    if(args.length<3){
      System.err.println(
        """
          |Usage: DirectKafkaWordCount <brokers> <topics> <groupid>
          |  <brokers> is a list of one or more Kafka brokers
          |  <topics> is a list of one or more kafka topics to consume from
          |  <groupid> is a consume group
          |
          |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    //赋值参数
    val Array(brokers,topics,groupId)=args

    //Create context with 2 second batch interval
    val sparkConf=new SparkConf().setAppName("DirectKafkaWordCount")
    //启动多个executor
    sparkConf.setMaster("local[*]")
    //设置rdd的每个分区每秒最多可以从kafka的partition上拉取多少条数据
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","5")
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val ssc=new StreamingContext(sparkConf,Seconds(2))

    //设置访问kafka的一些参数
    val topicsSet=topics.split(",").toSet
    val kafkaParams=Map[String,String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest",
    "zookeeper.connect" ->"mini1:2181,mini2:2181,mini3:2181"
    )

    //创建一个自定义和KafkaUtils功能一样的实例
    val km=new KafkaManager(kafkaParams)
    //创建一个direct方式的获取数据的流
    val messages=km.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)

    //这里的message是一个流，由于是自定义的方式，想要使它转换为RDD，需使用一下方式
    messages.foreachRDD(rdd =>{
      if(!rdd.isEmpty()){
        //先处理消息
        processRdd(rdd)
        //再更新offsets
        km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
