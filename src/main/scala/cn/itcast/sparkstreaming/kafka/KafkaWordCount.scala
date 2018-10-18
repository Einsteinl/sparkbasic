package cn.itcast.sparkstreaming.kafka

import cn.itcast.sparkstreaming.LoggerLevels
import cn.itcast.sparkstreaming.kafka.demo.RedisClient
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.JedisPool

object KafkaWordCount {

  val updateFunc=(iter: Iterator[(String,Seq[Int],Option[Int])]) =>{
    //iter.flatMap(it => Some(it._2.sum+it._3.getOrElse(0)).map(x =>(it._1,x)))
    iter.flatMap{case(x,y,z) => Some(y.sum+z.getOrElse(0)).map(i=>(x,i))}

  }

  //启动所传的参数： mini1:2181,mini2:2181,mini3:2181 gg test1 2
  def main(args: Array[String]): Unit = {
    //设置打印的日志级别
    LoggerLevels.setStreamingLogLevels()
    //zookeeper地址，消费者组，有那些topic,几个消费者
    val Array(zkQuorum,group,topics,numThreads)=args
    val sparkConf=new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("c://ck2")
    //指定每个topic和它的消费者个数
    val topicMap=topics.split(",").map((_,numThreads.toInt)).toMap
    //获取kafka传过来Dstream
    val data=KafkaUtils.createStream(ssc,zkQuorum,group,topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    //kafka 里存储的数据都是kv形式，这里map(_._2)获得value，进行计算
    val words=data.map(_._2).flatMap(_.split(" "))
    val wordCounts=words.map((_,1)).updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    wordCounts.mapPartitions(it =>{
      //获取Jedis连接
      val jedis = RedisClient.pool.getResource
      it.map(word =>{
        //利用redis Set结构，将分区中每个单词和它的次数，存入redis中
        jedis.sadd(word._1,word._2.toString)

      })
      //计算完每个分区将连接归还
      RedisClient.pool.returnResource(jedis)
      //返回分区
      it
    })
    ssc.start()
    ssc.awaitTermination()
  }

}

object RedisClient extends Serializable {
  val redisHost = "mini1"
  val redisPort = 6379
  val redisTimeout = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}
