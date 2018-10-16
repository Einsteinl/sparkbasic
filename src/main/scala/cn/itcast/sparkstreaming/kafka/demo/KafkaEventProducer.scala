package cn.itcast.sparkstreaming.kafka.demo

import java.util.Properties

import com.google.gson.{Gson, JsonObject}


import scala.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

import scala.util.Random

/**
  * Kafka+Spark Streaming+Redis编程实践
  */

/**
  * 1、手机客户端会收集用户的行为事件（我们以点击事件为例），
  * 将数据发送到数据服务器，我们假设这里直接进入到Kafka消息队列
  * 2、后端的实时服务会从Kafka消费数据，将数据读出来并进行实时分析，
  * 这里选择Spark Streaming，因为Spark Streaming提供了与Kafka整合的内置支持
  * 3、经过Spark Streaming实时计算程序分析，将结果写入Redis，可以实时获取用户的行为数据，
  * 并可以导出进行离线综合统计分析
  */

/**
  * 写了一个Kafka Producer模拟程序，用来模拟向Kafka实时写入用户行为的事件数据，数据是JSON格式
  *
  * {
  * "uid": "068b746ed4620d25e26055a9f804385f",
  * "event_time": "1430204612405",
  * "os_type": "Android",
  * "click_count": 6
  * }
  */

/**
  * 一个事件包含4个字段：
  * 　　1、uid：用户编号
  * 　　2、event_time：事件发生时间戳
  * 　　3、os_type：手机App操作系统类型
  * 　　4、click_count：点击次数
  */
object KafkaEventProducer {

  private val users = Array(
    "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
    "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
    "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
    "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
    "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d")

  private val random = new Random()

  private var pointer = -1

  //随机用户Id
  def getUserID() : String = {
    pointer = pointer + 1
    if(pointer >= users.length) {
      pointer = 0
      users(pointer)
    } else {
      users(pointer)
    }
  }

  //随机生成单击次数
  def click() : Double = {
    random.nextInt(10)
  }

  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
  // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
  // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
  def main(args: Array[String]): Unit = {
    val topic = "user_events"
    //broker的ip地址
    val brokers = "mini1:9092,mini2:9092,mini3:9092"
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    //new一个生产者
    val producer = new Producer[String, String](kafkaConfig)

    while(true) {
      // prepare event data

      val event = new JsonObject()
      event.addProperty("uid", getUserID)
      event.addProperty("event_time", System.currentTimeMillis.toString)
      event.addProperty("os_type", "Android")
      event.addProperty("click_count", click)

      // produce event message
      producer.send(new KeyedMessage[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(200)
    }
  }
}
