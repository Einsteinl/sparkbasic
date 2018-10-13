package cn.itcast.spark.demo02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 与UserLocation不同之处：
  * 1.手机号与基站组合用了元组，而它用的是字符串拼接
  * 2.分组累计求和的时候，用的是reduceByKey,而它是先按照拼接的字符串分组，之后用mapValues对value集合进行累计求和
  * 3.由于是元组嵌套，所以直接可以用下标将各个字段提取出来，它则是字符串连接，需要切分
  * 3.加载了基站信息，以基站Id为key
  * 4.进行join操作，拿到了基站的坐标信息，并将其填入到信息中
  */
object AdvUserLocation {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("AdvUserLocation").setMaster("local")
    val sc=new SparkContext(conf)
    //把手机号和基站连在一起，准备分组，设置进入基站时间为负，走出基站时间为正
    val rdd0=sc.textFile("c://bs_log").map(line =>{
      val fields=line.split(",")
      val eventType=fields(3)
      val time=fields(1)
      val timeLong=if(eventType=="1") -time.toLong else time.toLong
      //把手机号和基站放到一个元组中
      ((fields(0),fields(2)),timeLong)
    })

    //分组，算出呆了多长时间,之后，以基站key 准备匹配基站坐标信息
    val rdd1=rdd0.reduceByKey(_+_).map(t =>{
      val mobile=t._1._1
      val lac=t._1._2
      val time=t._2
      (lac,(mobile,time))
    })


    val rdd2=sc.textFile("c://loc_info.txt").map(line =>{
      val f=line.split(",")
      //(基站Id，(经度，纬度))
      (f(0),(f(1),f(2)))
    })

    //匹配基站坐标,并对内部进行切分处理
    val rdd3=rdd1.join(rdd2).map(t =>{
      val lac=t._1
      val mobile=t._2._1._1
      val time=t._2._1._2
      val x=t._2._2._1
      val y=t._2._2._2
      (mobile,lac,time,x,y)
    })

    //println(rdd3.collect().toBuffer)

    //对手机进行分组，对时间进行排序
    val rdd4=rdd3.groupBy(_._1)

    val rdd5=rdd4.mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(2)
    })
    rdd5.saveAsTextFile("c://out")
    sc.stop()
  }

}
