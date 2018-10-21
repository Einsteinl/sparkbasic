package cn.itcast.gamedemo

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{KafkaManager, KafkaUtils}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

/**
  * 游戏的扫描插件 direct方式不成功
  */
object PlugsScanner {

  def processRdd(rdd: RDD[(String,Long)]) : Unit={

    rdd.foreachPartition(it => {
      lazy val jedis : Jedis=JedisConnectionPool.getConnection()
      it.foreach(t =>{
        val account=t._1
        val time=System.currentTimeMillis()
        println(account)
        jedis.set(account,"")
      })
      jedis.close()
    })

  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val Array(brokers,topics,groupId)= Array("mini1:9092,mini2:9092,mini3:9092","gamelogs","g1")
    //val Array(zkQuorum,group,topics,numThreads)=Array("mini1:2181,mini2:2181,mini3:2181","g3","gamelogs","2")

    val dateFormat=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf=new SparkConf().setAppName("PlugsScanner").setMaster("local[2]")
    conf.set("spark.streaming.kafka.maxRatePerPartition","100")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc=new SparkContext(conf)
    //10秒一个batch
    val ssc=new StreamingContext(sc,Milliseconds(10000))

    //receiver方式拉取数据
//    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap
//    val dstream=KafkaUtils.createStream(ssc,zkQuorum,group,topicMap,StorageLevel.MEMORY_AND_DISK_SER)

    //direct方式拉取数据
    val topicsSet=topics.split(",").toSet
    val kafkaParams=Map[String,String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
    val km=new KafkaManager(kafkaParams)

    val dstream=km.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topicsSet)

    //拿到kafka message
    val lines=dstream.map(_._2)
    //对message进行切分
    val fields=lines.map(_.split("\t"))

    //得到使用太阳水的所有数据
    val dsl=fields.filter(field =>{
      field(3).equals("11") && field(8).equals("强效太阳水")
    })

    //每隔10秒计算出最近30秒 以使用太阳水的时间为value按照用户名分组
    val ds2=dsl.map(f => (f(7),dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000),Milliseconds(10000)).filter(_._2.size>2)

    //计算每个用户使用太阳水的平均时间
    val ds3=ds2.mapValues(it =>{
      val list=it.toList.sorted
      val size=list.size
      val first=list(0)
      val last=list(size -1)
      (last- first)/size
    })

    //将使用太阳水平均时间小于一秒的用户过滤出来
    val ds4=ds3.filter(_._2 <1000L)

    //将这些用户存入到redis中，供游戏引擎参考，将其踢出
    ds4.foreachRDD(rdd =>{
      if(!rdd.isEmpty()){
        //先处理消息
        processRdd(rdd)
        //km.updateZKOffsets(rdd)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
