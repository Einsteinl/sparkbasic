package cn.itcast.gamedemo

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 游戏的扫描插件
  */
object ScannPlugins {

  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics, numThreads) = Array("node-1.itcast.cn:2181,node-2.itcast.cn:2181,node-3.itcast.cn:2181", "g0", "gamelogs", "1")
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("ScannPlugins").setMaster("local[4]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Milliseconds(10000))
    //receiver方式设置checkpoint
    sc.setCheckpointDir("c://ck0")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val dstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val lines = dstream.map(_._2)
    val splitedLines = lines.map(_.split("\t"))
    //将使用太阳水的数据过滤出来
    val filteredLines = splitedLines.filter(f => {
      val et = f(3)
      val item = f(8)
      et == "11" && item == "强效太阳水"
    })

    //以用户使用太阳水的时间为value对用户进行分组(用户名,使用太阳水的时间)
    val grouedWindow: DStream[(String, Iterable[Long])] = filteredLines.map(f => (f(7), dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000), Milliseconds(20000))
    val filtered: DStream[(String, Iterable[Long])] = grouedWindow.filter(_._2.size >= 5)

    //计算出每个用户使用太阳水的平均时间
    val itemAvgTime = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size - 1)
      val cha: Double = last - first
      cha / size
    })

    //按照用户使用太阳水的平均时间判断出该用户是否使用外挂
    val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2 < 10000)

    //将每个使用外挂的用户存储到redis当中
    badUser.foreachRDD(rdd => {
      //一个分区获取一个jedis连接
      rdd.foreachPartition(it => {
        val connection = JedisConnectionPool.getConnection()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user + "_" + currentTime, avgTime.toString)
        })
        connection.close()
      })
    })



    filteredLines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
